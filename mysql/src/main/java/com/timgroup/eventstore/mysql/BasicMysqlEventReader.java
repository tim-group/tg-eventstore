package com.timgroup.eventstore.mysql;

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

    public BasicMysqlEventReader(ConnectionProvider connectionProvider, String tableName, int batchSize) {
        this.connectionProvider = connectionProvider;
        this.tableName = tableName;
        this.batchSize = batchSize;
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        EventSpliterator spliterator = new EventSpliterator(
                connectionProvider,
                batchSize,
                tableName,
                (BasicMysqlEventStorePosition) positionExclusive,
                "",
                false,
                false
        );

        return stream(spliterator, false);
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
        EventSpliterator spliterator = new EventSpliterator(
                connectionProvider,
                theBatchSize,
                tableName,
                positionExclusive,
                "",
                true,
                false
        );

        return stream(spliterator, false);
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
