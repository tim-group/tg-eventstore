package com.timgroup.eventstore.mysql.legacy;

import com.codahale.metrics.Timer;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.mysql.ConnectionProvider;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static java.lang.Integer.MIN_VALUE;
import static java.lang.Long.MAX_VALUE;

final class LegacyMysqlEventSpliterator implements Spliterator<ResolvedEvent> {
    private final ConnectionProvider connectionProvider;
    private final StreamId pretendStreamId;
    private final String queryString;

    private LegacyMysqlEventPosition lastPosition;
    private final Optional<Timer> timer;
    private Iterator<ResolvedEvent> currentPage = Collections.emptyIterator();
    private boolean streamExhausted = false;

    LegacyMysqlEventSpliterator(ConnectionProvider connectionProvider, int batchSize, String tableName, StreamId pretendStreamId, LegacyMysqlEventPosition startingPosition, boolean backwards, Optional<Timer> timer) {
        this.connectionProvider = connectionProvider;
        this.pretendStreamId = pretendStreamId;
        this.lastPosition = startingPosition;
        this.timer = timer;
        this.queryString = "select version, effective_timestamp, eventType, body" +
                " from " + tableName +
                " where version " + (backwards ? "<" : ">") + " %s" +
                " order by version " + (backwards ? "desc" : "asc") +
                " limit " + batchSize;
    }

    @Override
    public boolean tryAdvance(Consumer<? super ResolvedEvent> action) {
        if (!currentPage.hasNext() && !streamExhausted) {
            try (Timer.Context c = timer.map(Timer::time).orElse(new Timer().time())) {
                try (Connection connection = connectionProvider.getConnection();
                     Statement statement = streamingStatementFrom(connection);
                     ResultSet resultSet = statement.executeQuery(String.format(queryString, lastPosition.legacyVersion))
                ) {
                    List<ResolvedEvent> list = new ArrayList<>();

                    while (resultSet.next()) {
                        LegacyMysqlEventPosition position = LegacyMysqlEventPosition.fromLegacyVersion(resultSet.getLong("version"));
                        Timestamp effectiveTimestamp = resultSet.getTimestamp("effective_timestamp");
                        list.add(new ResolvedEvent(
                                position,
                                eventRecord(
                                        effectiveTimestamp.toInstant(),
                                        pretendStreamId,
                                        position.toEventNumber(),
                                        resultSet.getString("eventType"),
                                        resultSet.getBytes("body"),
                                        LegacyMysqlMetadataCodec.metadataFrom(effectiveTimestamp)
                                )));
                    }
                    currentPage = list.iterator();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
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
