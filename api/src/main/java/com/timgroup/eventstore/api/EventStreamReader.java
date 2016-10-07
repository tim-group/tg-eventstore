package com.timgroup.eventstore.api;

import java.util.stream.Stream;

public interface EventStreamReader {
    long EmptyStreamEventNumber = -1;

    default Stream<ResolvedEvent> readStreamForwards(StreamId streamId) {
        return readStreamForwards(streamId, EmptyStreamEventNumber);
    }

    Stream<ResolvedEvent> readStreamForwards(StreamId streamId, long eventNumber);
}
