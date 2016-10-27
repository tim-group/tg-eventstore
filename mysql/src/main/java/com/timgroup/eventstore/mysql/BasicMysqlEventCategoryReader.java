package com.timgroup.eventstore.mysql;

import java.util.stream.Stream;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;

import static com.timgroup.eventstore.mysql.BasicMysqlEventStorePosition.EMPTY_STORE_POSITION;
import static java.lang.String.format;
import static java.util.stream.StreamSupport.stream;

public class BasicMysqlEventCategoryReader implements EventCategoryReader {
    private final ConnectionProvider connectionProvider;
    private final String tableName;
    private final int batchSize;

    public BasicMysqlEventCategoryReader(ConnectionProvider connectionProvider, String tableName, int batchSize) {
        this.connectionProvider = connectionProvider;
        this.tableName = tableName;
        this.batchSize = batchSize;
    }

    @Override
    public Stream<ResolvedEvent> readCategoryForwards(String category, Position positionExclusive) {
        EventSpliterator spliterator = new EventSpliterator(
                connectionProvider,
                batchSize,
                tableName,
                (BasicMysqlEventStorePosition) positionExclusive,
                format("stream_category = '%s'", category));

        return stream(spliterator, false);
    }

    @Override
    public Position emptyCategoryPosition(String category) {
        return EMPTY_STORE_POSITION;
    }
}
