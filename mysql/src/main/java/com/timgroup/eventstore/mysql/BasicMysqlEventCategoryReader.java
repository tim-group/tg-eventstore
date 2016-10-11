package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.stream.StreamSupport.stream;

public class BasicMysqlEventCategoryReader implements EventCategoryReader {
    private final ConnectionProvider connectionProvider;
    private final String tableName;

    public BasicMysqlEventCategoryReader(ConnectionProvider connectionProvider, String tableName) {
        this.connectionProvider = connectionProvider;
        this.tableName = tableName;
    }

    @Override
    public Stream<ResolvedEvent> readCategoryForwards(String category) {
        return readCategoryForwards(category, new BasicMysqlEventStorePosition(-1));
    }

    @Override
    public Stream<ResolvedEvent> readCategoryForwards(String category, Position position) {
        BasicMysqlEventStorePosition basicMysqlEventStorePosition = (BasicMysqlEventStorePosition) position;

        EventSpliterator spliterator = new EventSpliterator(connectionProvider,
                format("select position, timestamp, stream_category, stream_id, event_number, event_type, data, metadata " +
                        "from %s where stream_category = '%s' and position > %s order by position asc", tableName, category, basicMysqlEventStorePosition.value));

        return stream(spliterator, false).onClose(spliterator::close);
    }
}
