package com.timgroup.eventstore.api;

import java.util.stream.Stream;

public interface EventReader {
    default Stream<ResolvedEvent> readAllForwards() {
        return readAllForwards(emptyStorePosition());
    }

    Stream<ResolvedEvent> readAllForwards(Position positionExclusive);

    Position emptyStorePosition();
}
