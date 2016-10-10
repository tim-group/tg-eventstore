package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.StreamId;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;

public class BasicMysqlEventStreamWriter implements EventStreamWriter {
    private final ConnectionProvider connectionProvider;

    public BasicMysqlEventStreamWriter(ConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
    }

    @Override
    public void write(StreamId streamId, Collection<NewEvent> events) {
        try (Connection connection = connectionProvider.getConnection();
             PreparedStatement statement = connection.prepareStatement("insert into event(timestamp, stream_category, stream_id, event_number, event_type, data, metadata) values(UTC_TIMESTAMP(), ?, ?, ?, ?, ?, ?)")
        ) {

            //todo: deal with transactions
            //todo: deal with locking tables

            long eventNumber = 0;

            for (NewEvent event : events) {
                statement.setString(1, streamId.category());
                statement.setString(2, streamId.id());
                statement.setLong(3, eventNumber++);
                statement.setString(4, event.type());
                statement.setBytes(5, event.data());
                statement.setBytes(6, event.metadata());
                statement.addBatch();
            }

            int[] ints = statement.executeBatch();

            //todo: deal with affected rows
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(StreamId streamId, Collection<NewEvent> events, long expectedVersion) {
    }
}
