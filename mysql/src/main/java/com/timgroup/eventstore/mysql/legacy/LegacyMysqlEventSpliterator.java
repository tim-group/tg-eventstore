package com.timgroup.eventstore.mysql.legacy;

import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.mysql.ConnectionProvider;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.function.Consumer;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static java.lang.Integer.MIN_VALUE;
import static java.lang.Long.MAX_VALUE;

final class LegacyMysqlEventSpliterator implements Spliterator<ResolvedEvent> {
    private final ConnectionProvider connectionProvider;
    private final StreamId pretendStreamId;
    private final String queryString;

    private LegacyMysqlEventPosition lastPosition;
    private Iterator<ResolvedEvent> currentPage = Collections.emptyIterator();
    private boolean streamExhausted = false;

    LegacyMysqlEventSpliterator(ConnectionProvider connectionProvider, int batchSize, String tableName, StreamId pretendStreamId, LegacyMysqlEventPosition startingPosition, boolean backwards) {
        this.connectionProvider = connectionProvider;
        this.pretendStreamId = pretendStreamId;
        this.lastPosition = startingPosition;
        this.queryString = "select version, effective_timestamp, eventType, body" +
                " from " + tableName +
                " where version " + (backwards ? "<" : ">") + " %s" +
                " order by version " + (backwards ? "desc" : "asc") +
                " limit " + batchSize;
    }

    @Override
    public boolean tryAdvance(Consumer<? super ResolvedEvent> action) {
        if (!currentPage.hasNext() && !streamExhausted) {
            try (Connection connection = connectionProvider.getConnection();
                 Statement statement = streamingStatementFrom(connection);
                 ResultSet resultSet = statement.executeQuery(String.format(queryString, lastPosition.legacyVersion))
            ) {
                List<ResolvedEvent> list = new ArrayList<>();

                while (resultSet.next()) {
                    LegacyMysqlEventPosition position = LegacyMysqlEventPosition.fromLegacyVersion(resultSet.getLong("version"));
                    list.add(new ResolvedEvent(
                            position,
                            eventRecord(
                                    resultSet.getTimestamp("effective_timestamp").toInstant(),
                                    pretendStreamId,
                                    position.toEventNumber(),
                                    resultSet.getString("eventType"),
                                    resultSet.getBytes("body"),
                                    new byte[0]
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
            lastPosition = ((LegacyMysqlEventPosition) next.position());
            return true;
        } else {
            streamExhausted = true;
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
