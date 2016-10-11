package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static java.lang.Integer.MIN_VALUE;
import static java.lang.Long.MAX_VALUE;
import static java.lang.String.format;
import static java.util.stream.StreamSupport.stream;

public class BasicMysqlEventStreamReader implements EventStreamReader {
    private final ConnectionProvider connectionProvider;
    private final String tableName;

    public BasicMysqlEventStreamReader(ConnectionProvider connectionProvider, String tableName) {
        this.connectionProvider = connectionProvider;
        this.tableName = tableName;
    }

    @Override
    public Stream<ResolvedEvent> readStreamForwards(StreamId streamId, long eventNumber) {
        //todo: handling of transactions

        EventSpliterator spliterator = new EventSpliterator(connectionProvider,
                format("select position, timestamp, stream_category, stream_id, event_number, event_type, data, metadata " +
                        "from %s where stream_category = '%s' and stream_id = '%s' and event_number > %s", tableName, streamId.category(), streamId.id(), eventNumber));

        return stream(spliterator, false).onClose(spliterator::close);
    }

    private static final class BasicMysqlEventStorePosition implements Position {
        private final long value;

        private BasicMysqlEventStorePosition(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return Long.toString(value);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof BasicMysqlEventStorePosition)) return false;

            BasicMysqlEventStorePosition that = (BasicMysqlEventStorePosition) o;

            return value == that.value;

        }

        @Override
        public int hashCode() {
            return (int) (value ^ (value >>> 32));
        }
    }

    private static class EventSpliterator implements Spliterator<ResolvedEvent>, AutoCloseable {
        private Connection connection = null;
        private Statement statement = null;
        private ResultSet resultSet = null;

        public EventSpliterator(ConnectionProvider connectionProvider, String query) {
            try {
                connection = connectionProvider.getConnection();
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
                    //TODO: log this?
                }
                resultSet = null;
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    //TODO: log this?
                }
                statement = null;
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    //TODO: log this?
                }
                connection = null;
            }
        }
    }
}
