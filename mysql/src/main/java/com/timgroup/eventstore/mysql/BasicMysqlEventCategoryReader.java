package com.timgroup.eventstore.mysql;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.timgroup.eventstore.mysql.BasicMysqlEventStorePosition.EMPTY_STORE_POSITION;
import static java.util.Objects.requireNonNull;
import static java.util.stream.StreamSupport.stream;

@SuppressWarnings({"OptionalUsedAsFieldOrParameterType", "WeakerAccess"})
public class BasicMysqlEventCategoryReader implements EventCategoryReader {
    private final ConnectionProvider connectionProvider;
    private final String tableName;
    private final int batchSize;
    private final Optional<Timer> timer;
    private final Optional<Timer> multiCategoryTimer;

    public BasicMysqlEventCategoryReader(ConnectionProvider connectionProvider, String databaseName, String tableName, int batchSize, @Nullable MetricRegistry metricRegistry) {
        this.connectionProvider = requireNonNull(connectionProvider);
        this.tableName = requireNonNull(tableName);
        this.batchSize = batchSize;
        this.timer = Optional.ofNullable(metricRegistry).map(r -> r.timer(String.format("database.%s.%s.read_category.page_fetch_time", databaseName, tableName)));
        this.multiCategoryTimer = Optional.ofNullable(metricRegistry).map(r -> r.timer(String.format("database.%s.%s.read_categories.page_fetch_time", databaseName, tableName)));
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public Stream<ResolvedEvent> readCategoryForwards(String category, Position positionExclusive) {
        return stream(EventSpliterator.readCategoryEventSpliterator(
                connectionProvider,
                batchSize,
                tableName,
                category,
                (BasicMysqlEventStorePosition) positionExclusive,
                false,
                timer
        ), false);
    }

    @Nonnull
    @Override
    public Stream<ResolvedEvent> readCategoriesForwards(List<String> categories, Position positionExclusive) {
        return stream(new EventSpliterator<>(
                connectionProvider,
                (BasicMysqlEventStorePosition) positionExclusive,
                pos -> categories.stream().map(category -> "(select position, timestamp, stream_category, stream_id, event_number, event_type, data, metadata" +
                        " from " + tableName +
                        " FORCE INDEX (stream_category_2)" +
                        " where position > " + pos.value +
                        " and stream_category = '" + category + "'" +
                        " order by position asc" +
                        " limit " + batchSize + ")").collect(Collectors.joining(" union all ", "", " order by position asc limit " + batchSize)),
                resolvedEvent -> (BasicMysqlEventStorePosition) resolvedEvent.position(),
                multiCategoryTimer), false);
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public Stream<ResolvedEvent> readCategoryBackwards(String category) {
        return readCategoryBackwards(category, new BasicMysqlEventStorePosition(Long.MAX_VALUE));
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public Stream<ResolvedEvent> readCategoryBackwards(String category, Position positionExclusive) {
        return readBackwards(category, (BasicMysqlEventStorePosition) positionExclusive, this.batchSize);
    }

    @Nonnull
    @Override
    public Optional<ResolvedEvent> readLastEventInCategory(String category) {
        return readBackwards(category, new BasicMysqlEventStorePosition(Long.MAX_VALUE), 1).findFirst();
    }

    @Nonnull
    @Override
    public Position emptyCategoryPosition(String category) {
        return EMPTY_STORE_POSITION;
    }

    @Nonnull
    @Override
    public PositionCodec categoryPositionCodec(String category) {
        return BasicMysqlEventStorePosition.CODEC;
    }

    @Override
    public String toString() {
        return "BasicMysqlEventCategoryReader{" +
                "tableName='" + tableName + '\'' +
                ", batchSize=" + batchSize +
                '}';
    }

    private Stream<ResolvedEvent> readBackwards(String category, BasicMysqlEventStorePosition positionExclusive, int theBatchSize) {
        return stream(EventSpliterator.readCategoryEventSpliterator(
                connectionProvider,
                theBatchSize,
                tableName,
                category,
                positionExclusive,
                true,
                timer), false);
    }
}
