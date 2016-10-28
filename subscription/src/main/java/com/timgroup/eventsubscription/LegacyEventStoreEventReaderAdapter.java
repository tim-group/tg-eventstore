package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.EventInStream;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventStore;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.LegacyPositionAdapter;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;

import java.time.Instant;
import java.util.stream.Stream;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static com.timgroup.eventstore.api.StreamId.streamId;

public class LegacyEventStoreEventReaderAdapter implements EventReader, EventStreamReader {
    private final EventStore eventStore;

    public LegacyEventStoreEventReaderAdapter(EventStore eventStore) {
        this.eventStore = eventStore;
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards() {
        return eventStore.streamingFromAll(0).map(this::toResolvedEvent);
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        return eventStore.streamingFromAll(((LegacyPositionAdapter) positionExclusive).version()).map(this::toResolvedEvent);
    }

    @Override
    public Position emptyStorePosition() {
        return new LegacyPositionAdapter(0);
    }

    private ResolvedEvent toResolvedEvent(EventInStream eventInStream) {
        return new ResolvedEvent(
                new LegacyPositionAdapter(eventInStream.version()),
                eventRecord(
                        Instant.ofEpochMilli(eventInStream.effectiveTimestamp().getMillis()),
                        streamId("all", "all"),
                        eventInStream.version(),
                        eventInStream.eventData().eventType(),
                        eventInStream.eventData().body().data(),
                        new byte[0]
                )
        );
    }

    @Override
    public Stream<ResolvedEvent> readStreamForwards(StreamId streamId, long eventNumber) {
        if (!streamId.equals(streamId("all", "all"))) {
            throw new IllegalArgumentException("Cannot read " + streamId + " using legacy adapter");
        }
        return eventStore.streamingFromAll(eventNumber).map(this::toResolvedEvent);
    }
}
