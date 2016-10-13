package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.api.WrongExpectedVersionException;

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
            connection.setAutoCommit(false);
            long currentEventNumber = currentEventNumber(streamId, connection);

            write(streamId, events, currentEventNumber, connection);

            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(StreamId streamId, Collection<NewEvent> events, long expectedVersion) {
        try (Connection connection = connectionProvider.getConnection()) {
            connection.setAutoCommit(false);

            long currentEventNumber = currentEventNumber(streamId, connection);

            if (currentEventNumber != expectedVersion) {
                throw new WrongExpectedVersionException(currentEventNumber, expectedVersion);
            }

            write(streamId, events, currentEventNumber, connection);
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void write(StreamId streamId, Collection<NewEvent> events, long currentEventNumber, Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("insert into " + tableName + "(position, timestamp, stream_category, stream_id, event_number, event_type, data, metadata) values(?, UTC_TIMESTAMP(), ?, ?, ?, ?, ?, ?)")) {

            long currentPosition = currentPosition(connection);

            long eventNumber = currentEventNumber;

            for (NewEvent event : events) {
                statement.setLong(1, ++currentPosition);
                statement.setString(2, streamId.category());
                statement.setString(3, streamId.id());
                statement.setLong(4, ++eventNumber);
                statement.setString(5, event.type());
                statement.setBytes(6, event.data());
                statement.setBytes(7, event.metadata());
                statement.addBatch();
            }

            int[] affectedRows = statement.executeBatch();

            if (affectedRows.length != events.size()) {
                throw new RuntimeException("Expected to write " + events.size() + " events but wrote " + affectedRows.length);
            }
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
