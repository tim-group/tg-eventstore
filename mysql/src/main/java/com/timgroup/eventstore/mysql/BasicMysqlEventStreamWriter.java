package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.api.WrongExpectedVersion;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static java.lang.String.format;

public class BasicMysqlEventStreamWriter implements EventStreamWriter {
    private final ConnectionProvider connectionProvider;
    private final String tableName;

    public BasicMysqlEventStreamWriter(ConnectionProvider connectionProvider, String tableName) {
        this.connectionProvider = connectionProvider;
        this.tableName = tableName;
    }

    @Override
    public void write(StreamId streamId, Collection<NewEvent> events) {
        try (Connection connection = connectionProvider.getConnection()) {
            write(streamId, events, currentEventNumber(streamId, connection), connection);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(StreamId streamId, Collection<NewEvent> events, long expectedVersion) {
        try (Connection connection = connectionProvider.getConnection()) {
            long currentEventNumber = currentEventNumber(streamId, connection);

            if (currentEventNumber != expectedVersion) {
                throw new WrongExpectedVersion(currentEventNumber, expectedVersion);
            }

            write(streamId, events, currentEventNumber, connection);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void write(StreamId streamId, Collection<NewEvent> events, long currentEventNumber, Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("insert into " + tableName + "(timestamp, stream_category, stream_id, event_number, event_type, data, metadata) values(UTC_TIMESTAMP(), ?, ?, ?, ?, ?, ?)")) {
            //todo: deal with transactions
            //todo: deal with locking tables

            long eventNumber = currentEventNumber;

            for (NewEvent event : events) {
                statement.setString(1, streamId.category());
                statement.setString(2, streamId.id());
                statement.setLong(3, ++eventNumber);
                statement.setString(4, event.type());
                statement.setBytes(5, event.data());
                statement.setBytes(6, event.metadata());
                statement.addBatch();
            }

            int[] affectedRows = statement.executeBatch();

            if (affectedRows.length != events.size()) {
                throw new RuntimeException("Expected to write " + events.size() + " events but wrote " + affectedRows.length);
            }
        }
    }

    private long currentEventNumber(StreamId streamId, Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(format("select event_number from %s where stream_category = ? and stream_id = ? order by event_number desc limit 1", tableName))) {
            statement.setString(1, streamId.category());
            statement.setString(2, streamId.id());
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getLong("event_number");
                } else {
                    return -1;
                }
            }
        }
    }
}
