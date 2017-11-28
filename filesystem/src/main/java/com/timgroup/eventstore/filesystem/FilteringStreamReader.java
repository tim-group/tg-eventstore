package com.timgroup.eventstore.filesystem;

import java.util.function.Predicate;
import java.util.stream.Stream;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.NoSuchStreamException;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;

final class FilteringStreamReader implements EventStreamReader {
    private final EventReader underlying;
    private final Predicate<? super StreamId> streamExistsPredicate;

    public FilteringStreamReader(EventReader underlying, Predicate<? super StreamId> streamExistsPredicate) {
        this.underlying = underlying;
        this.streamExistsPredicate = streamExistsPredicate;
    }

    @Override
    public Stream<ResolvedEvent> readStreamForwards(StreamId streamId, long eventNumber) {
        if (!streamExistsPredicate.test(streamId)) throw new NoSuchStreamException(streamId);
        return underlying.readAllForwards()
                .filter(re -> re.eventRecord().streamId().equals(streamId) && re.eventRecord().eventNumber() > eventNumber);
    }

    @Override
    public Stream<ResolvedEvent> readStreamBackwards(StreamId streamId) {
        if (!streamExistsPredicate.test(streamId)) throw new NoSuchStreamException(streamId);
        return underlying.readAllBackwards()
                .filter(re -> re.eventRecord().streamId().equals(streamId));
    }

    @Override
    public Stream<ResolvedEvent> readStreamBackwards(StreamId streamId, long eventNumber) {
        if (!streamExistsPredicate.test(streamId)) throw new NoSuchStreamException(streamId);
        return underlying.readAllBackwards()
                .filter(re -> re.eventRecord().streamId().equals(streamId) && re.eventRecord().eventNumber() < eventNumber);
    }

    @Override
    public String toString() {
        return "FilteringStreamReader{" +
                "underlying=" + underlying +
                ", streamExistsPredicate=" + streamExistsPredicate +
                '}';
    }
}
