package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.NoSuchStreamException;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

import static com.timgroup.eventstore.mysql.BasicMysqlEventStorePosition.EMPTY_STORE_POSITION;
import static java.lang.String.format;
import static java.util.stream.StreamSupport.stream;

public class BasicMysqlEventStreamReader implements EventStreamReader {
    private final ConnectionProvider connectionProvider;
    private final String tableName;
    private final int batchSize;

    public BasicMysqlEventStreamReader(ConnectionProvider connectionProvider, String tableName, int batchSize) {
        this.connectionProvider = connectionProvider;
        this.tableName = tableName;
        this.batchSize = batchSize;
    }

    @Override
    public Stream<ResolvedEvent> readStreamForwards(StreamId streamId, long eventNumber) {
        ensureStreamExists(streamId);
        return stream(EventSpliterator.readStreamEventSpliterator(
                connectionProvider,
                batchSize,
                tableName,
                streamId,
                eventNumber,
                false
        ), false);
    }

    @Override
    public Stream<ResolvedEvent> readStreamBackwards(StreamId streamId) {
        return readStreamBackwards(streamId, Long.MAX_VALUE);
    }

    @Override
    public Stream<ResolvedEvent> readStreamBackwards(StreamId streamId, long eventNumber) {
        return readBackwards(streamId, eventNumber, this.batchSize);
    }

    @Override
    public ResolvedEvent readLastEventInStream(StreamId streamId) {
        //noinspection ConstantConditions
        return readBackwards(streamId, Long.MAX_VALUE, 1).findFirst().get();
    }

    @Override
    public String toString() {
        return "BasicMysqlEventStreamReader{" +
                "tableName='" + tableName + '\'' +
                ", batchSize=" + batchSize +
                '}';
    }

    private Stream<ResolvedEvent> readBackwards(StreamId streamId, long eventNumber, int theBatchSize) {
        ensureStreamExists(streamId);

        return stream(EventSpliterator.readStreamEventSpliterator(
                connectionProvider,
                theBatchSize,
                tableName,
                streamId,
                eventNumber,
                true
        ), false);
    }

    private void ensureStreamExists(StreamId streamId) throws NoSuchStreamException {
        try (Connection connection = connectionProvider.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(String.format("select position from %s where stream_category = '%s' and stream_id = '%s' limit 1", tableName, streamId.category(), streamId.id()))
        ) {
            if (!resultSet.first()) {
                throw new NoSuchStreamException(streamId);
            }
        } catch (SQLException e) {
            throw new RuntimeException(String.format("Error checking whether stream '%s' exists", streamId), e);
        }
    }
}
