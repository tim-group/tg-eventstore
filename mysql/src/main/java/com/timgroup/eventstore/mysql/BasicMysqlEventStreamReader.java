package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.NoSuchStreamException;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;

import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.stream.StreamSupport.stream;

public class BasicMysqlEventStreamReader implements EventStreamReader {
    private final ConnectionProvider connectionProvider;
    private final String tableName;
    private final int batchSize;

    public BasicMysqlEventStreamReader(ConnectionProvider connectionProvider, String tableName, int batchSize) {
        this.connectionProvider = connectionProvider;
        this.tableName = tableName;
        this.batchSize = batchSize;
    }

    @Override
    public Stream<ResolvedEvent> readStreamForwards(StreamId streamId, long eventNumber) {
        EventSpliterator spliterator = new EventSpliterator(connectionProvider, batchSize, tableName, new BasicMysqlEventStorePosition(-1), String.format("stream_category = '%s' and stream_id = '%s' and event_number > %s", streamId.category(), streamId.id(), eventNumber));

        return stream(new NotEmptySpliterator<>(spliterator, streamId), false);
    }

    private static final class NotEmptySpliterator<T> implements Spliterator<T> {
        private final Spliterator<T> underlying;
        private final StreamId streamId;
        private boolean seenValue;

        private NotEmptySpliterator(Spliterator<T> underlying, StreamId streamId) {
            this.underlying = underlying;
            this.streamId = streamId;
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            if (seenValue) {
                return underlying.tryAdvance(action);
            }
            if (!underlying.tryAdvance(action)) {
                throw new NoSuchStreamException(streamId);
            }
            seenValue = true;
            return true;
        }

        @Override
        public Spliterator<T> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return underlying.estimateSize();
        }

        @Override
        public int characteristics() {
            return underlying.characteristics();
        }
    }
}
