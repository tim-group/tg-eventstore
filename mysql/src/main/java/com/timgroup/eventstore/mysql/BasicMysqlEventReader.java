package com.timgroup.eventstore.mysql;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.Optional;
import java.util.stream.Stream;

import static com.timgroup.eventstore.mysql.BasicMysqlEventStorePosition.EMPTY_STORE_POSITION;
import static java.util.stream.StreamSupport.stream;

public class BasicMysqlEventReader implements EventReader {
    private final ConnectionProvider connectionProvider;
    private final String tableName;
    private final int batchSize;
    private final Optional<Timer> timer;

    public BasicMysqlEventReader(ConnectionProvider connectionProvider, String databaseName, String tableName, int batchSize, MetricRegistry metricRegistry) {
        this.connectionProvider = connectionProvider;
        this.tableName = tableName;
        this.batchSize = batchSize;
        this.timer = Optional.ofNullable(metricRegistry).map(r -> r.timer(String.format("database.%s.%s.read_all.page_fetch_time", databaseName, tableName)));
    }

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

    @Override
    public Stream<ResolvedEvent> readAllBackwards() {
        return readAllBackwards(new BasicMysqlEventStorePosition(Long.MAX_VALUE));
    }

    @Override
    public Stream<ResolvedEvent> readAllBackwards(Position positionExclusive) {
        return readBackwards((BasicMysqlEventStorePosition) positionExclusive, this.batchSize);
    }

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

    @Override
    public Position emptyStorePosition() {
        return EMPTY_STORE_POSITION;
    }

    @Override
    public String toString() {
        return "BasicMysqlEventReader{" +
                "tableName='" + tableName + '\'' +
                ", batchSize=" + batchSize +
                '}';
    }
}
