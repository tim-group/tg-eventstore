package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Spliterator;
import java.util.function.Consumer;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static java.lang.Integer.MIN_VALUE;
import static java.lang.Long.MAX_VALUE;
import static org.slf4j.LoggerFactory.getLogger;

class EventSpliterator implements Spliterator<ResolvedEvent>, AutoCloseable {
    private static final Logger LOG = getLogger(EventSpliterator.class);

    private Connection connection = null;
    private Statement statement = null;
    private ResultSet resultSet = null;

    EventSpliterator(ConnectionProvider connectionProvider, String query) {
        try {
            connection = connectionProvider.getConnection();
            connection.setAutoCommit(false);
            statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            statement.setFetchSize(MIN_VALUE);
            resultSet = statement.executeQuery(query);
        } catch (SQLException e) {
            close();
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean tryAdvance(Consumer<? super ResolvedEvent> action) {
        try {
            if (resultSet.next()) {
                action.accept(new ResolvedEvent(
                        new BasicMysqlEventStorePosition(resultSet.getLong("position")),
                        eventRecord(
                                resultSet.getTimestamp("timestamp").toInstant(),
                                StreamId.streamId(resultSet.getString("stream_category"), resultSet.getString("stream_id")),
                                resultSet.getLong("event_number"),
                                resultSet.getString("event_type"),
                                resultSet.getBytes("data"),
                                resultSet.getBytes("metadata")
                        )
                ));
                return true;
            } else {
                close();
                return false;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Spliterator<ResolvedEvent> trySplit() {
        return null;
    }

    @Override
    public long estimateSize() {
        return MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return ORDERED | NONNULL | DISTINCT;
    }

    @Override
    public void close() {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                LOG.warn("Failure closing ResultSet. Ignoring.", e);
            }
            resultSet = null;
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                LOG.warn("Failure closing Statement. Ignoring.", e);
            }
            statement = null;
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOG.warn("Failure closing Connection. Ignoring.", e);
            }
            connection = null;
        }
    }
}
