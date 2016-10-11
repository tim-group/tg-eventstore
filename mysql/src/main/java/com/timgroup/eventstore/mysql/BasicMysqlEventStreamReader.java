package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;

import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.stream.StreamSupport.stream;

public class BasicMysqlEventStreamReader implements EventStreamReader {
    private final ConnectionProvider connectionProvider;
    private final String tableName;

    public BasicMysqlEventStreamReader(ConnectionProvider connectionProvider, String tableName) {
        this.connectionProvider = connectionProvider;
        this.tableName = tableName;
    }

    @Override
    public Stream<ResolvedEvent> readStreamForwards(StreamId streamId, long eventNumber) {
        EventSpliterator spliterator = new EventSpliterator(connectionProvider,
                format("select position, timestamp, stream_category, stream_id, event_number, event_type, data, metadata " +
                        "from %s where stream_category = '%s' and stream_id = '%s' and event_number > %s order by position asc", tableName, streamId.category(), streamId.id(), eventNumber));

        return stream(spliterator, false).onClose(spliterator::close);
    }

}
