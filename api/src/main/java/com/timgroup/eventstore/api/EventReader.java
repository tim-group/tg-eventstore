package com.timgroup.eventstore.api;

import java.util.stream.Stream;

public interface EventReader {
    Stream<ResolvedEvent> readStreamForwards(StreamId streamId);

    Stream<ResolvedEvent> readStreamForwards(StreamId streamId, long eventNumber);

    Stream<ResolvedEvent> readCategoryForwards(String category);

    Stream<ResolvedEvent> readCategoryForwards(String category, Position position);

    Stream<ResolvedEvent> readAllForwards();

    Stream<ResolvedEvent> readAllForwards(Position positionExclusive);
}
