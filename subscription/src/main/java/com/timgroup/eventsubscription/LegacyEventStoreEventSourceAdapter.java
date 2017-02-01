package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.*;
import com.timgroup.eventstore.api.legacy.LegacyEventStoreEventStreamWriterAdapter;
import com.timgroup.tucker.info.Component;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Stream;

import static com.timgroup.eventstore.api.StreamId.streamId;
import static java.util.Spliterators.spliteratorUnknownSize;

public final class LegacyEventStoreEventSourceAdapter implements EventSource {
    private final EventStore eventStore;
    private final LegacyEventStoreEventReaderAdapter eventReader;
    private final LegacyEventStoreEventStreamWriterAdapter eventStreamWriter;
    private final LegacyEventReaderEventCategoryReaderAdapter eventCategoryReader;
    private final LegacyPositionAdapterCodec positionCodec;

    public LegacyEventStoreEventSourceAdapter(EventStore eventStore) {
        this.eventStore = eventStore;
        this.eventReader = new LegacyEventStoreEventReaderAdapter(eventStore);
        this.eventStreamWriter = new LegacyEventStoreEventStreamWriterAdapter(eventStore);
        this.eventCategoryReader = new  LegacyEventReaderEventCategoryReaderAdapter(eventReader);
        this.positionCodec = new LegacyPositionAdapterCodec();
    }

    @Override
    public EventReader readAll() {
        return eventReader;
    }

    @Override
    public EventStreamReader readStream() {
        return eventReader;
    }

    @Override
    public EventStreamWriter writeStream() {
        return eventStreamWriter;
    }

    @Override
    public EventCategoryReader readCategory() {
        return eventCategoryReader;
    }

    @Override
    public PositionCodec positionCodec() {
        return positionCodec;
    }

    @Override
    public Collection<Component> monitoring() {
        return Collections.emptyList();
    }

    private static final class LegacyEventReaderEventCategoryReaderAdapter implements EventCategoryReader {
        private final LegacyEventStoreEventReaderAdapter eventReader;

        private LegacyEventReaderEventCategoryReaderAdapter(LegacyEventStoreEventReaderAdapter eventReader) {
            this.eventReader = eventReader;
        }

        @Override
        public Stream<ResolvedEvent> readCategoryForwards(String category, Position positionExclusive) {
            return eventReader.readStreamForwards(streamId(category, "all"));
        }

        @Override
        public Position emptyCategoryPosition(String category) {
            return eventReader.emptyStorePosition();
        }
    }

    private static final class LegacyPositionAdapterCodec implements PositionCodec {
        @Override
        public Position deserializePosition(String serialisedPosition) {
            return new LegacyPositionAdapter(Long.parseLong(serialisedPosition));
        }

        @Override
        public String serializePosition(Position position) {
            return Long.toString(((LegacyPositionAdapter) position).version());
        }
    }

}
