package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.function.Consumer;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static java.lang.Integer.MIN_VALUE;
import static java.lang.Long.MAX_VALUE;

class EventSpliterator implements Spliterator<ResolvedEvent> {
    private final int batchSize;
    private final String tableName;
    private final ConnectionProvider connectionProvider;

    private BasicMysqlEventStorePosition lastPosition;
    private final String condition;
    private Iterator<ResolvedEvent> currentPage = Collections.emptyIterator();

    EventSpliterator(ConnectionProvider connectionProvider, int batchSize, String tableName, BasicMysqlEventStorePosition startingPosition, String condition) {
        this.connectionProvider = connectionProvider;
        this.batchSize = batchSize;
        this.tableName = tableName;
        this.lastPosition = startingPosition;
        this.condition = condition;
    }

    @Override
    public boolean tryAdvance(Consumer<? super ResolvedEvent> action) {
        if (!currentPage.hasNext()) {
            try (Connection connection = connectionProvider.getConnection();
                 Statement statement = streamingStatementFrom(connection);
                 ResultSet resultSet = statement.executeQuery(String.format("select position, timestamp, stream_category, stream_id, event_number, event_type, data, metadata " +
                         "from %s where %s position > %s order by position asc limit %d", tableName, condition.isEmpty() ? "" : condition + " and ", lastPosition.value, batchSize));
            ) {

                List<ResolvedEvent> list = new ArrayList<>();

                while (resultSet.next()) {
                    list.add(new ResolvedEvent(
                            new BasicMysqlEventStorePosition(resultSet.getLong("position")),
                            eventRecord(
                                    resultSet.getTimestamp("timestamp").toInstant(),
                                    StreamId.streamId(resultSet.getString("stream_category"), resultSet.getString("stream_id")),
                                    resultSet.getLong("event_number"),
                                    resultSet.getString("event_type"),
                                    resultSet.getBytes("data"),
                                    resultSet.getBytes("metadata")
                            )));
                    currentPage = list.iterator();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }

        if (currentPage.hasNext()) {
            ResolvedEvent next = currentPage.next();
            action.accept(next);
            lastPosition = ((BasicMysqlEventStorePosition) next.position());
            return true;
        } else {
            return false;
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

    private static Statement streamingStatementFrom(Connection connection) throws SQLException {
        Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        statement.setFetchSize(MIN_VALUE);
        return statement;
    }
}
