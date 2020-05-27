package com.timgroup.eventstore.mysql;

import com.codahale.metrics.Timer;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static java.lang.Integer.MIN_VALUE;
import static java.lang.Long.MAX_VALUE;

class EventSpliterator<T> implements Spliterator<ResolvedEvent> {
    private final ConnectionProvider connectionProvider;
    private final Function<T, String> queryStringGenerator;
    private final Function<ResolvedEvent, T> locationPointerExtractor;
    private final Optional<Timer> timer;

    private T locationPointer;
    private Iterator<ResolvedEvent> currentPage = Collections.emptyIterator();
    private boolean streamExhausted = false;

    private static Instant readInstant(ResultSet rs, String columnName) throws SQLException {
        return rs.getTimestamp(columnName).toLocalDateTime().toInstant(ZoneOffset.UTC);
    }

    public static Spliterator<ResolvedEvent> readAllEventSpliterator(ConnectionProvider connectionProvider,
                                                                     int batchSize,
                                                                     String tableName,
                                                                     BasicMysqlEventStorePosition startingPosition,
                                                                     boolean backwards, Optional<Timer> timer)
    {
        final String queryString = "select position, timestamp, stream_category, stream_id, event_number, event_type, data, metadata" +
                " from " + tableName +
                " where position " + (backwards ? "<" : ">") + " %s" +
                " order by position " + (backwards ? "desc" : "asc") +
                " limit " + batchSize;

        return new EventSpliterator<>(
                connectionProvider,
                startingPosition,
                position -> String.format(queryString, position.value),
                resolvedEvent -> (BasicMysqlEventStorePosition)resolvedEvent.position(),
                timer);
    }

    public static Spliterator<ResolvedEvent> readCategoryEventSpliterator(ConnectionProvider connectionProvider,
                                                                          int batchSize,
                                                                          String tableName,
                                                                          String category,
                                                                          BasicMysqlEventStorePosition startingPosition,
                                                                          boolean backwards,
                                                                          Optional<Timer> timer)
    {
        final String queryString = "select position, timestamp, stream_category, stream_id, event_number, event_type, data, metadata" +
                " from " + tableName +
                " FORCE INDEX (stream_category_2)" +
                " where position " + (backwards ? "<" : ">") + " %s" +
                " and stream_category = '" + category + "'" +
                " order by position " + (backwards ? "desc" : "asc") +
                " limit " + batchSize;

        return new EventSpliterator<>(
                connectionProvider,
                startingPosition,
                position -> String.format(queryString, position.value),
                resolvedEvent -> (BasicMysqlEventStorePosition) resolvedEvent.position(),
                timer);
    }


    public static Spliterator<ResolvedEvent> readStreamEventSpliterator(ConnectionProvider connectionProvider,
                                                                        int batchSize,
                                                                        String tableName,
                                                                        StreamId streamId,
                                                                        long startingEventNumber,
                                                                        boolean backwards,
                                                                        Optional<Timer> timer)
    {
        final String queryString = "select position, timestamp, stream_category, stream_id, event_number, event_type, data, metadata" +
                " from " + tableName +
                " where event_number " + (backwards ? "<" : ">") + " %d" +
                " and stream_category = '" + streamId.category() + "'" +
                " and stream_id = '" + streamId.id() + "'" +
                " order by event_number " + (backwards ? "desc" : "asc") +
                " limit " + batchSize;

        return new EventSpliterator<>(
                connectionProvider,
                startingEventNumber,
                eventNumber -> String.format(queryString, eventNumber),
                resolvedEvent -> resolvedEvent.eventRecord().eventNumber(),
                timer
        );
    }

    EventSpliterator(
            ConnectionProvider connectionProvider,
            T startingLocation,
            Function<T, String> queryStringGenerator,
            Function<ResolvedEvent, T> locationPointerExtractor,
            Optional<Timer> timer)
    {
        this.connectionProvider = connectionProvider;
        this.locationPointer = startingLocation;
        this.queryStringGenerator = queryStringGenerator;
        this.locationPointerExtractor = locationPointerExtractor;
        this.timer = timer;
    }

    @Override
    public boolean tryAdvance(Consumer<? super ResolvedEvent> action) {
        if (!currentPage.hasNext() && !streamExhausted) {
            try (Timer.Context c = timer.map(t -> t.time()).orElse(new Timer().time());) {
                try (Connection connection = connectionProvider.getConnection();
                     Statement statement = streamingStatementFrom(connection);
                     ResultSet resultSet = statement.executeQuery(queryStringGenerator.apply(locationPointer))
                ) {

                    List<ResolvedEvent> list = new ArrayList<>();

                    while (resultSet.next()) {
                        list.add(new ResolvedEvent(
                                new BasicMysqlEventStorePosition(resultSet.getLong("position")),
                                eventRecord(
                                        readInstant(resultSet, "timestamp"),
                                        StreamId.streamId(resultSet.getString("stream_category"), resultSet.getString("stream_id")),
                                        resultSet.getLong("event_number"),
                                        resultSet.getString("event_type"),
                                        resultSet.getBytes("data"),
                                        resultSet.getBytes("metadata")
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
            locationPointer = locationPointerExtractor.apply(next);
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
