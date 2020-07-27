package com.timgroup.eventstore.mysql;

import com.codahale.metrics.MetricRegistry;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.NoSuchStreamException;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import io.prometheus.client.Histogram;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

import static com.timgroup.eventstore.mysql.BasicMysqlEventReader.pageFetchTimer;
import static java.util.Objects.requireNonNull;
import static java.util.stream.StreamSupport.stream;

@ParametersAreNonnullByDefault
public class BasicMysqlEventStreamReader implements EventStreamReader {

    private final static Histogram streamValidationTimer = Histogram.build("tg_eventstore_ensure_stream_exists_validation_seconds", "TG Eventstore ensure stream exists validation")
            .labelNames("database", "table")
            .register();

    private final ConnectionProvider connectionProvider;
    private final String tableName;
    private final int batchSize;
    private final Timer timer;
    private final Timer validationTimer;

    public BasicMysqlEventStreamReader(ConnectionProvider connectionProvider, String databaseName, String tableName, int batchSize, @Nullable MetricRegistry metricRegistry) {
        this.connectionProvider = requireNonNull(connectionProvider);
        this.tableName = requireNonNull(tableName);
        this.batchSize = batchSize;
        if (metricRegistry == null) {
            this.timer = (Runnable r) -> pageFetchTimer.labels(databaseName, tableName, "read_stream").time(r);
            this.validationTimer = (Runnable r) -> streamValidationTimer.labels(databaseName, tableName).time(r);
        } else {
            this.timer = metricRegistry.timer(String.format("database.%s.%s.read_stream.page_fetch_time", databaseName, tableName))::time;
            this.validationTimer = metricRegistry.timer(String.format("database.%s.%s.ensure_stream_exists_validation.time", databaseName, tableName))::time;
        }
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public Stream<ResolvedEvent> readStreamForwards(StreamId streamId, long eventNumber) {
        ensureStreamExists(streamId);
        return stream(EventSpliterator.readStreamEventSpliterator(
                connectionProvider,
                batchSize,
                tableName,
                streamId,
                eventNumber,
                false,
                timer
        ), false);
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public Stream<ResolvedEvent> readStreamBackwards(StreamId streamId) {
        return readStreamBackwards(streamId, Long.MAX_VALUE);
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public Stream<ResolvedEvent> readStreamBackwards(StreamId streamId, long eventNumber) {
        return readBackwards(streamId, eventNumber, this.batchSize);
    }

    @Nonnull
    @Override
    public ResolvedEvent readLastEventInStream(StreamId streamId) {
        //noinspection ConstantConditions
        return readBackwards(streamId, Long.MAX_VALUE, 1).findFirst().get();
    }

    @Nonnull
    @Override
    public PositionCodec streamPositionCodec() {
        return BasicMysqlEventStorePosition.CODEC;
    }

    @Override
    public String toString() {
        return "BasicMysqlEventStreamReader{" +
                "tableName='" + tableName + '\'' +
                ", batchSize=" + batchSize +
                '}';
    }

    //select position from event force index(stream_category) where stream_category = 'received_files' and stream_id = 'DataScopeEM' limit 1;

    private Stream<ResolvedEvent> readBackwards(StreamId streamId, long eventNumber, int theBatchSize) {
        ensureStreamExists(streamId);

        return stream(EventSpliterator.readStreamEventSpliterator(
                connectionProvider,
                theBatchSize,
                tableName,
                streamId,
                eventNumber,
                true,
                timer), false);
    }

    private void ensureStreamExists(StreamId streamId) throws NoSuchStreamException {
        validationTimer.time(() -> {
            try (Connection connection = connectionProvider.getConnection();
                 Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(String.format("select position from %s force index(stream_category) where stream_category = '%s' and stream_id = '%s' limit 1", tableName, streamId.category(), streamId.id()))
            ) {
                if (!resultSet.first()) {
                    throw new NoSuchStreamException(streamId);
                }
            } catch (SQLException e) {
                throw new RuntimeException(String.format("Error checking whether stream '%s' exists", streamId), e);
            }
        });
    }
}
