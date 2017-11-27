package com.timgroup.eventstore.api;

import java.util.Optional;
import java.util.stream.Stream;

public interface EventStreamReader {
    long EmptyStreamEventNumber = -1;

    default Stream<ResolvedEvent> readStreamForwards(StreamId streamId) {
        return readStreamForwards(streamId, EmptyStreamEventNumber);
    }

    Stream<ResolvedEvent> readStreamForwards(StreamId streamId, long eventNumber);

    default Stream<ResolvedEvent> readStreamBackwards(StreamId streamId) {
        throw new UnsupportedOperationException("reading backwards is not yet supported");
    }

    default Optional<ResolvedEvent> readLastEventInStream(StreamId streamId) {
        return readStreamBackwards(streamId).findFirst();
    }

    default Stream<ResolvedEvent> readStreamBackwards(StreamId streamId, long eventNumber) {
        throw new UnsupportedOperationException("reading backwards is not yet supported");
    }
}
