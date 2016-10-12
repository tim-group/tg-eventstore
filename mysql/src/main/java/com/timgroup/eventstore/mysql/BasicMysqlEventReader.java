package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;

import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.stream.StreamSupport.stream;

public class BasicMysqlEventReader implements EventReader {
    private static final BasicMysqlEventStorePosition EMPTY_STORE_POSITION = new BasicMysqlEventStorePosition(-1);
    private final ConnectionProvider connectionProvider;
    private final String tableName;

    public BasicMysqlEventReader(ConnectionProvider connectionProvider, String tableName) {
        this.connectionProvider = connectionProvider;
        this.tableName = tableName;
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards() {
        return readAllForwards(EMPTY_STORE_POSITION);
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        BasicMysqlEventStorePosition basicMysqlEventStorePosition = (BasicMysqlEventStorePosition) positionExclusive;

        EventSpliterator spliterator = new EventSpliterator(connectionProvider,
                format("select position, timestamp, stream_category, stream_id, event_number, event_type, data, metadata " +
                        "from %s where position > %s order by position asc", tableName, basicMysqlEventStorePosition.value));

        return stream(spliterator, false).onClose(spliterator::close);
    }

    public static Position emptyStorePosition() {
        return EMPTY_STORE_POSITION;
    }
}
