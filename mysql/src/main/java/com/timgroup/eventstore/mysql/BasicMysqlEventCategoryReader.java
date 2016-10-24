package com.timgroup.eventstore.mysql;

import java.util.stream.Stream;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;

import static java.lang.String.format;
import static java.util.stream.StreamSupport.stream;

public class BasicMysqlEventCategoryReader implements EventCategoryReader {
    private static final BasicMysqlEventStorePosition EMPTY_CATEGORY_POSITION = new BasicMysqlEventStorePosition(-1);
    private final ConnectionProvider connectionProvider;
    private final String tableName;

    public BasicMysqlEventCategoryReader(ConnectionProvider connectionProvider, String tableName) {
        this.connectionProvider = connectionProvider;
        this.tableName = tableName;
    }

    @Override
    public Stream<ResolvedEvent> readCategoryForwards(String category, Position position) {
        BasicMysqlEventStorePosition basicMysqlEventStorePosition = (BasicMysqlEventStorePosition) position;

        EventSpliterator spliterator = new EventSpliterator(connectionProvider,
                format("select position, timestamp, stream_category, stream_id, event_number, event_type, data, metadata " +
                        "from %s where stream_category = '%s' and position > %s order by position asc", tableName, category, basicMysqlEventStorePosition.value));

        return stream(spliterator, false).onClose(spliterator::close);
    }

    @Override
    public Position emptyCategoryPosition(String category) {
        return EMPTY_CATEGORY_POSITION;
    }
}
