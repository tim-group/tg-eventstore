package com.timgroup.eventstore.api;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.util.stream.Stream;

public interface EventStreamReader {
    long EmptyStreamEventNumber = -1;

    @Nonnull
    @CheckReturnValue
    default Stream<ResolvedEvent> readStreamForwards(StreamId streamId) {
        return readStreamForwards(streamId, EmptyStreamEventNumber);
    }

    @Nonnull
    @CheckReturnValue
    Stream<ResolvedEvent> readStreamForwards(StreamId streamId, long eventNumber);

    @Nonnull
    @CheckReturnValue
    default Stream<ResolvedEvent> readStreamBackwards(StreamId streamId) {
        throw new UnsupportedOperationException("reading backwards is not yet supported");
    }

    @Nonnull
    @CheckReturnValue
    default ResolvedEvent readLastEventInStream(StreamId streamId) {
        //noinspection ConstantConditions
        return readStreamBackwards(streamId).findFirst().get();
    }

    @Nonnull
    @CheckReturnValue
    default Stream<ResolvedEvent> readStreamBackwards(StreamId streamId, long eventNumber) {
        throw new UnsupportedOperationException("reading backwards is not yet supported");
    }
}
