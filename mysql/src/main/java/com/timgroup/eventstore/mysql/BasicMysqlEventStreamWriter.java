package com.timgroup.eventstore.mysql;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.api.WrongExpectedVersionException;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

@ParametersAreNonnullByDefault
public class BasicMysqlEventStreamWriter implements EventStreamWriter {
    private final ConnectionProvider connectionProvider;
    private final String tableName;
    private final Optional<Timer> timer;
    private final Optional<Histogram> histogram;
    private final Optional<Counter> retryCounter;

    public BasicMysqlEventStreamWriter(ConnectionProvider connectionProvider, String databaseName, String tableName, @Nullable MetricRegistry metricRegistry) {
        this.connectionProvider = requireNonNull(connectionProvider);
        this.tableName = requireNonNull(tableName);
        this.timer = Optional.ofNullable(metricRegistry).map(r -> r.timer(String.format("database.%s.%s.write.time", databaseName, tableName)));
        this.histogram = Optional.ofNullable(metricRegistry).map(r -> r.histogram(String.format("database.%s.%s.write.count", databaseName, tableName)));
        this.retryCounter = Optional.ofNullable(metricRegistry).map(r -> r.counter(String.format("database.%s.%s.retry.count", databaseName, tableName)));
    }

    @Override
    public void write(StreamId streamId, Collection<NewEvent> events) {
        execute(singletonList(new StreamWriteRequest(streamId, events, OptionalLong.empty())));
    }

    @Override
    public void write(StreamId streamId, Collection<NewEvent> events, long expectedVersion) {
        execute(singletonList(new StreamWriteRequest(streamId, events, OptionalLong.of(expectedVersion))));
    }

    @Override
    public void execute(Collection<StreamWriteRequest> writeRequests) {
        if (writeRequests.stream().allMatch(r -> r.events.isEmpty())) {
            return;
        }

        writeRequests.stream().collect(toMap(r -> r.streamId, r -> r, (r1, r2) -> {
            throw new RuntimeException("Duplicate streamId in write request: " + r1.streamId);
        }));

        try (Connection connection = connectionProvider.getConnection()) {
            connection.setAutoCommit(false);

            Map<StreamId, Long> currentEventNumbers = currentEventNumbers(writeRequests, connection);

            List<String> failures = new ArrayList<>();

            List<WritableEvent> events = new ArrayList<>();

            for (StreamWriteRequest req : writeRequests) {
                long currentEventNumber = currentEventNumbers.getOrDefault(req.streamId, -1L);

                if (req.expectedVersion.isPresent() && req.expectedVersion.getAsLong() != currentEventNumber) {
                    failures.add(req.streamId + ": " + "current version: " + currentEventNumber + ", expected version: " + req.expectedVersion.getAsLong());
                    continue;
                }

                long eventNumber = currentEventNumber;

                for (NewEvent event : req.events) {
                    events.add(new WritableEvent(
                            req.streamId,
                            ++eventNumber,
                            event.type(),
                            event.data(),
                            event.metadata()
                    ));
                }
            }

            write(events, connection);

            connection.commit();

            if (!failures.isEmpty()) {
                throw new WrongExpectedVersionException(failures.stream().collect(joining(",")));
            }
        } catch (BatchUpdateException e) {
            if (e.getCause() instanceof SQLIntegrityConstraintViolationException) {
                retryCounter.ifPresent(Counter::inc);
                execute(writeRequests); // retry indefinitely
            } else {
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "BasicMysqlEventStreamWriter{" +
                "tableName='" + tableName + '\'' +
                '}';
    }

    private void write(Collection<WritableEvent> events, Connection connection) throws SQLException {
        try (
                Timer.Context c = timer.map(t -> t.time()).orElse(new Timer().time());
                PreparedStatement statement = connection.prepareStatement(
                        "insert into " + tableName + "(position, timestamp, stream_category, stream_id, event_number, event_type, data, metadata) " +
                        "values(?, " + currentTime(connection.getMetaData()) +", ?, ?, ?, ?, ?, ?)"
                )
        ) {

            long currentPosition = currentPosition(connection);

            for (WritableEvent event : events) {
                statement.setLong(1, ++currentPosition);
                statement.setString(2, event.streamId.category());
                statement.setString(3, event.streamId.id());
                statement.setLong(4, event.eventNumber);
                statement.setString(5, event.eventType);
                statement.setBytes(6, event.data);
                statement.setBytes(7, event.metadata);
                statement.addBatch();
            }

            int[] affectedRows = statement.executeBatch();

            if (affectedRows.length != events.size()) {
                throw new RuntimeException("Expected to write " + events.size() + " events but wrote " + affectedRows.length);
            }
            histogram.ifPresent(h -> h.update(events.size()));
        }
    }

    private static String currentTime(DatabaseMetaData meta) throws SQLException {
        return BasicMysqlEventStoreSetup.mysqlSupportsFractionalSecondsForDatetime(meta)
                ? "UTC_TIMESTAMP(6)"
                : "UTC_TIMESTAMP()";
    }

    private long currentPosition(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(format("select max(position) as current_position from %s", tableName))) {
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getLong("current_position");
            }
        }
    }

    private Map<StreamId, Long> currentEventNumbers(Collection<StreamWriteRequest> writeRequests, Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(format("select event_number from %s where stream_category = ? and stream_id = ? order by event_number desc limit 1", tableName))) {
            Map<StreamId, Long> eventNumbers = new HashMap<>();

            for (StreamWriteRequest r : writeRequests) {
                statement.setString(1, r.streamId.category());
                statement.setString(2, r.streamId.id());
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        eventNumbers.put(r.streamId, resultSet.getLong(1));
                    }
                }
                statement.clearParameters();
            }

            return eventNumbers;
        }
    }

    private static class WritableEvent {
        private final StreamId streamId;
        private final long eventNumber;
        private final String eventType;
        private final byte[] data;
        private final byte[] metadata;

        private WritableEvent(StreamId streamId, long eventNumber, String eventType, byte[] data, byte[] metadata) {
            this.streamId = streamId;
            this.eventNumber = eventNumber;
            this.eventType = eventType;
            this.data = data;
            this.metadata = metadata;
        }
    }
}
