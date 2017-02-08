package com.timgroup.eventstore.readerutils;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.time.Instant;
import java.util.stream.Stream;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;

public class BackdatingEventReader implements EventReader {
    private final EventReader underlying;
    private final Instant liveCutoverInclusive;

    public BackdatingEventReader(EventReader underlying, Instant liveCutoverInclusive) {
        this.underlying = underlying;
        this.liveCutoverInclusive = liveCutoverInclusive;
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        return underlying.readAllForwards(positionExclusive).map(this::possiblyBackdate);
    }

    private ResolvedEvent possiblyBackdate(ResolvedEvent resolvedEvent) {
        if (resolvedEvent.eventRecord().timestamp().isBefore(liveCutoverInclusive)) {
            EventRecord eventRecord = resolvedEvent.eventRecord();
            return new ResolvedEvent(resolvedEvent.position(),
                    eventRecord(Instant.EPOCH,
                            eventRecord.streamId(),
                            eventRecord.eventNumber(),
                            eventRecord.eventType(),
                            eventRecord.data(),
                            eventRecord.metadata()));
        } else {
            return resolvedEvent;
        }
    }

    @Override
    public Position emptyStorePosition() {
        return underlying.emptyStorePosition();
    }

    @Override
    public Stream<ResolvedEvent> readAllBackwards() {
        return underlying.readAllBackwards().map(this::possiblyBackdate);
    }

    @Override
    public Stream<ResolvedEvent> readAllBackwards(Position positionExclusive) {
        return underlying.readAllBackwards(positionExclusive).map(this::possiblyBackdate);
    }
}
