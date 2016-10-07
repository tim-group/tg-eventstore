package com.timgroup.eventstore.api;

import java.util.stream.Stream;

public interface EventStreamReader {
    Stream<ResolvedEvent> readStreamForwards(StreamId streamId);

    Stream<ResolvedEvent> readStreamForwards(StreamId streamId, long eventNumber);
}
