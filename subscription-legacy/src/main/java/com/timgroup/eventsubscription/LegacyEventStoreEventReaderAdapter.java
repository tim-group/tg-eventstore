package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.EventInStream;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventStore;
import com.timgroup.eventstore.api.EventStream;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.LegacyPositionAdapter;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.time.Instant;
import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static scala.collection.JavaConverters.asJavaIteratorConverter;

@ParametersAreNonnullByDefault
public class LegacyEventStoreEventReaderAdapter implements EventReader, EventStreamReader {
    private final EventStore eventStore;
    private final StreamId pretendStreamId;

    public LegacyEventStoreEventReaderAdapter(EventStore eventStore) {
        this(eventStore, streamId("all", "all"));
    }

    public LegacyEventStoreEventReaderAdapter(EventStore eventStore, StreamId pretendStreamId) {
        this.eventStore = eventStore;
        this.pretendStreamId = pretendStreamId;
    }

    @Nonnull
    @CheckReturnValue
    @Override
    public Stream<ResolvedEvent> readAllForwards() {
        return readAllForwards(0);
    }

    @Nonnull
    @CheckReturnValue
    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        long version = ((LegacyPositionAdapter) positionExclusive).version();
        return readAllForwards(version);
    }

    @Nonnull
    @CheckReturnValue
    @Override
    public Stream<ResolvedEvent> readStreamForwards(StreamId streamId, long eventNumber) {
        if (!streamId.equals(pretendStreamId)) {
            throw new IllegalArgumentException("Cannot read " + streamId + " using legacy adapter");
        }
        return readAllForwards(eventNumber);
    }

    private Stream<ResolvedEvent> readAllForwards(long version) {
        //TODO: streamingFromAll holds the SQL statement open for the duration, which causes blowup when processing is slow.
        return eventStore.streamingFromAll(version).map(this::toResolvedEvent);
    }

    public Stream<ResolvedEvent> readAllForwardsBuffered(Position positionExclusive) {
        EventStream eventStream = eventStore.fromAll(((LegacyPositionAdapter) positionExclusive).version());
        Iterator<EventInStream> events = asJavaIteratorConverter(eventStream).asJava();

        return StreamSupport.stream(spliteratorUnknownSize(events, ORDERED), false).map(this::toResolvedEvent);
    }

    @Nonnull
    @Override
    public Position emptyStorePosition() {
        return new LegacyPositionAdapter(0);
    }

    private ResolvedEvent toResolvedEvent(EventInStream eventInStream) {
        return new ResolvedEvent(
                new LegacyPositionAdapter(eventInStream.version()),
                eventRecord(
                        Instant.ofEpochMilli(eventInStream.effectiveTimestamp().getMillis()),
                        pretendStreamId,
                        eventInStream.version(),
                        eventInStream.eventData().eventType(),
                        eventInStream.eventData().body().data(),
                        new byte[0]
                )
        );
    }

    @Override
    public String toString() {
        return "LegacyEventStoreEventReaderAdapter{" +
                "eventStore=" + eventStore +
                ", pretendStreamId=" + pretendStreamId +
                '}';
    }
}
