package com.timgroup.eventstore.api;

import java.util.stream.Stream;

public interface EventCategoryReader {
    default Stream<ResolvedEvent> readCategoryForwards(String category) {
        return readCategoryForwards(category, emptyCategoryPosition(category));
    }

    Stream<ResolvedEvent> readCategoryForwards(String category, Position positionExclusive);

    Position emptyCategoryPosition(String category);
}
