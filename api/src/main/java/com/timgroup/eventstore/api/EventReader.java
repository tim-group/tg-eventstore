package com.timgroup.eventstore.api;

import java.util.stream.Stream;

public interface EventReader {
    default Stream<ResolvedEvent> readAllForwards() {
        return readAllForwards(emptyStorePosition());
    }

    Stream<ResolvedEvent> readAllForwards(Position positionExclusive);

    default Stream<ResolvedEvent> readAllBackwards() {
        throw new UnsupportedOperationException("reading backwards is not yet supported");
    }

    default Stream<ResolvedEvent> readAllBackwards(Position positionExclusive) {
        throw new UnsupportedOperationException("reading backwards is not yet supported");
    }

    Position emptyStorePosition();
}
