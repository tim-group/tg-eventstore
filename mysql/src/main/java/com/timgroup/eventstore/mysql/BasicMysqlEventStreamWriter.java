package com.timgroup.eventstore.mysql;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.api.WrongExpectedVersionException;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static com.timgroup.eventstore.api.StreamId.streamId;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

@ParametersAreNonnullByDefault
public class BasicMysqlEventStreamWriter implements EventStreamWriter {
    private final ConnectionProvider connectionProvider;
    private final String tableName;
    private final Optional<Timer> timer;
    private final Optional<Histogram> histogram;

    public BasicMysqlEventStreamWriter(ConnectionProvider connectionProvider, String databaseName, String tableName, @Nullable MetricRegistry metricRegistry) {
        this.connectionProvider = requireNonNull(connectionProvider);
        this.tableName = requireNonNull(tableName);
        this.timer = Optional.ofNullable(metricRegistry).map(r -> r.timer(String.format("database.%s.%s.write.time", databaseName, tableName)));
        this.histogram = Optional.ofNullable(metricRegistry).map(r -> r.histogram(String.format("database.%s.%s.write.count", databaseName, tableName)));
    }

    @Override
    public void write(StreamId streamId, Collection<NewEvent> events) {
        execute(singletonList(new StreamWriteRequest(streamId, OptionalLong.empty(), events)));
    }

    @Override
    public void write(StreamId streamId, Collection<NewEvent> events, long expectedVersion) {
        execute(singletonList(new StreamWriteRequest(streamId, OptionalLong.of(expectedVersion), events)));
    }

    @Override
    public void execute(Collection<StreamWriteRequest> writeRequests) {
        if (writeRequests.stream().mapToInt(r -> r.events.size()).sum() == 0) {
            return;
        }

        writeRequests.stream().collect(toMap(r -> r.streamId, r -> r, (r1, r2) -> {
            throw new RuntimeException("Duplicate streamId in write request: " + r1.streamId);
        }));

        try (Connection connection = connectionProvider.getConnection()) {
            connection.setAutoCommit(false);

            Map<StreamId, Long> currentEventNumbers = currentEventNumbers(writeRequests, connection);

            List<WritableEvent> events = new ArrayList<>();

            for (StreamWriteRequest req : writeRequests) {
                long currentEventNumber = currentEventNumbers.getOrDefault(req.streamId, -1L);

                req.expectedVersion.ifPresent(expectedVersion -> {
                    if (currentEventNumber != expectedVersion) {
                        throw new WrongExpectedVersionException(currentEventNumber, expectedVersion);
                    }
                });

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
                PreparedStatement statement = connection.prepareStatement("insert into " + tableName + "(position, timestamp, stream_category, stream_id, event_number, event_type, data, metadata) values(?, UTC_TIMESTAMP(), ?, ?, ?, ?, ?, ?)")
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

    private long currentPosition(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(format("select max(position) as current_position from %s", tableName))) {
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getLong("current_position");
            }
        }
    }

    private Map<StreamId, Long> currentEventNumbers(Collection<StreamWriteRequest> writeRequests, Connection connection) throws SQLException {
        String query = writeRequests.stream().map(s -> "(stream_category = ? and stream_id = ?)").collect(Collectors.joining(" or ", "select stream_category, stream_id, max(event_number) from " + tableName + " where", "group by stream_category, stream_id"));

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            int index = 1;

            for (StreamWriteRequest request : writeRequests) {
                statement.setString(index++, request.streamId.category());
                statement.setString(index++, request.streamId.id());
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                Map<StreamId, Long> eventNumbers = new HashMap<>();
                while (resultSet.next()) {
                    eventNumbers.put(streamId(resultSet.getString(1), resultSet.getString(2)), resultSet.getLong(3));
                }
                return eventNumbers;
            }
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
