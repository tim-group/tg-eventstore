package com.timgroup.eventstore.api;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.stream.Stream;

public interface EventReader {
    @Nonnull
    @CheckReturnValue
    default Stream<ResolvedEvent> readAllForwards() {
        return readAllForwards(emptyStorePosition());
    }

    @Nonnull
    @CheckReturnValue
    Stream<ResolvedEvent> readAllForwards(Position positionExclusive);

    @Nonnull
    @CheckReturnValue
    default Stream<ResolvedEvent> readAllBackwards() {
        throw new UnsupportedOperationException("reading backwards is not yet supported");
    }

    @Nonnull
    @CheckReturnValue
    default Stream<ResolvedEvent> readAllBackwards(Position positionExclusive) {
        throw new UnsupportedOperationException("reading backwards is not yet supported");
    }

    @Nonnull
    default Optional<ResolvedEvent> readLastEvent() {
        return readAllBackwards().findFirst();
    }

    @Nonnull
    Position emptyStorePosition();
}
