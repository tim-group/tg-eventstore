package com.timgroup.eventstore.mysql;

import com.codahale.metrics.MetricRegistry;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import io.prometheus.client.Histogram;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.stream.Stream;

import static com.timgroup.eventstore.mysql.BasicMysqlEventStorePosition.EMPTY_STORE_POSITION;
import static java.util.Objects.requireNonNull;
import static java.util.stream.StreamSupport.stream;

public class BasicMysqlEventReader implements EventReader {

    public final static Histogram pageFetchTimer = Histogram.build("tg_eventstore_page_fetch_seconds", "TG Eventstore page fetch time")
            .labelNames("database", "table", "read_type")
            .register();

    private final ConnectionProvider connectionProvider;
    private final String tableName;
    private final int batchSize;
    private final Timer timer;

    public BasicMysqlEventReader(ConnectionProvider connectionProvider, String databaseName, String tableName, int batchSize, @Nullable MetricRegistry metricRegistry) {
        this.connectionProvider = requireNonNull(connectionProvider);
        this.tableName = requireNonNull(tableName);
        this.batchSize = batchSize;
        if (metricRegistry == null) {
            this.timer = (Runnable r) -> pageFetchTimer.labels(databaseName, tableName, "read_all").time(r);
        } else {
            this.timer = metricRegistry.timer(String.format("database.%s.%s.read_all.page_fetch_time", databaseName, tableName))::time;
        }
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        return stream(EventSpliterator.readAllEventSpliterator(
                connectionProvider,
                batchSize,
                tableName,
                (BasicMysqlEventStorePosition) positionExclusive,
                false,
                timer
        ), false);
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public Stream<ResolvedEvent> readAllBackwards() {
        return readAllBackwards(new BasicMysqlEventStorePosition(Long.MAX_VALUE));
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public Stream<ResolvedEvent> readAllBackwards(Position positionExclusive) {
        return readBackwards((BasicMysqlEventStorePosition) positionExclusive, this.batchSize);
    }

    @Nonnull
    @Override
    public Optional<ResolvedEvent> readLastEvent() {
        return readBackwards(new BasicMysqlEventStorePosition(Long.MAX_VALUE), 1).findFirst();
    }

    private Stream<ResolvedEvent> readBackwards(BasicMysqlEventStorePosition positionExclusive, int theBatchSize) {
        return stream(EventSpliterator.readAllEventSpliterator(
                connectionProvider,
                theBatchSize,
                tableName,
                positionExclusive,
                true,
                timer), false);
    }

    @Nonnull
    @Override
    public Position emptyStorePosition() {
        return EMPTY_STORE_POSITION;
    }

    @Nonnull
    @Override
    public PositionCodec storePositionCodec() {
        return BasicMysqlEventStorePosition.CODEC;
    }

    @Override
    public String toString() {
        return "BasicMysqlEventReader{" +
                "tableName='" + tableName + '\'' +
                ", batchSize=" + batchSize +
                '}';
    }
}
