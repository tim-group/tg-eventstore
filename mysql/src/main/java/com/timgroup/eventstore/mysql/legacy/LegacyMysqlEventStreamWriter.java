package com.timgroup.eventstore.mysql.legacy;

import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.api.WrongExpectedVersionException;
import com.timgroup.eventstore.mysql.ConnectionProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static java.lang.String.format;

public final class LegacyMysqlEventStreamWriter implements EventStreamWriter {
    private final ConnectionProvider connectionProvider;
    private final String tableName;
    private final StreamId pretendStreamId;

    public LegacyMysqlEventStreamWriter(ConnectionProvider connectionProvider, String tableName, StreamId pretendStreamId) {
        this.connectionProvider = connectionProvider;
        this.tableName = tableName;
        this.pretendStreamId = pretendStreamId;
    }

    @Override
    public void write(StreamId streamId, Collection<NewEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        if (!streamId.equals(pretendStreamId)) {
            throw new IllegalArgumentException("Cannot write " + streamId + " to legacy store");
        }

        try (Connection connection = connectionProvider.getConnection()) {
            connection.setAutoCommit(false);

            write(connection, currentPosition(connection), events);

            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(StreamId streamId, Collection<NewEvent> events, long expectedEventNumber) {
        if (events.isEmpty()) {
            return;
        }
        if (!streamId.equals(pretendStreamId)) {
            throw new IllegalArgumentException("Cannot write " + streamId + " to legacy store");
        }

        try (Connection connection = connectionProvider.getConnection()) {
            connection.setAutoCommit(false);

            long currentPosition = currentPosition(connection);

            long currentEventNumber = currentPosition - 1;
            if (currentEventNumber != expectedEventNumber) {
                throw new WrongExpectedVersionException(currentEventNumber, expectedEventNumber);
            }

            write(connection, currentPosition, events);
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "LegacyMysqlEventStreamWriter{" +
                "connectionProvider=" + connectionProvider +
                ", tableName='" + tableName + '\'' +
                ", pretendStreamId=" + pretendStreamId +
                '}';
    }

    private void write(Connection connection, long currentPosition, Collection<NewEvent> events) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("insert into " + tableName + "(version, effective_timestamp, eventType, body) values(?, ?, ?, ?)")) {
            for (NewEvent event : events) {
                statement.setLong(1, ++currentPosition);
                statement.setTimestamp(2, LegacyMysqlMetadataCodec.effectiveTimestampFrom(event));
                statement.setString(3, event.type());
                statement.setBytes(4, event.data());
                statement.addBatch();
            }

            int[] affectedRows = statement.executeBatch();

            if (affectedRows.length != events.size()) {
                throw new RuntimeException("Expected to write " + events.size() + " events but wrote " + affectedRows.length);
            }
        }
    }

    private long currentPosition(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(format("select max(version) as current_position from %s", tableName))) {
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getLong("current_position");
            }
        }
    }
}
