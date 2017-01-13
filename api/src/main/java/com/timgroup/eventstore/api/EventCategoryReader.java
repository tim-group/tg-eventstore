package com.timgroup.eventstore.api;

import java.util.stream.Stream;

public interface EventCategoryReader {
    default Stream<ResolvedEvent> readCategoryForwards(String category) {
        return readCategoryForwards(category, emptyCategoryPosition(category));
    }

    Stream<ResolvedEvent> readCategoryForwards(String category, Position positionExclusive);

    default Stream<ResolvedEvent> readCategoryBackwards(String category) {
        throw new UnsupportedOperationException("reading backwards is not yet supported");
    }

    default Stream<ResolvedEvent> readCategoryBackwards(String category, Position positionExclusive) {
        throw new UnsupportedOperationException("reading backwards is not yet supported");
    }

    Position emptyCategoryPosition(String category);
}
