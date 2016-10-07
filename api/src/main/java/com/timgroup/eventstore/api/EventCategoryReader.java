package com.timgroup.eventstore.api;

import java.util.stream.Stream;

public interface EventCategoryReader {
    Stream<ResolvedEvent> readCategoryForwards(String category);

    Stream<ResolvedEvent> readCategoryForwards(String category, Position position);
}
