package com.timgroup.eventstore.api;

import java.util.stream.Stream;

public interface EventReader {
    Stream<ResolvedEvent> readAllForwards();

    Stream<ResolvedEvent> readAllForwards(Position positionExclusive);
}
