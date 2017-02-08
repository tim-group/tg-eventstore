package com.timgroup.eventstore.readerutils;

import com.timgroup.eventstore.api.*;

import java.util.stream.Stream;

public final class SingleEventCategoryEventReader implements EventReader {

    private final EventCategoryReader underlying;
    private final String category;

    public SingleEventCategoryEventReader(EventCategoryReader categoryReader, String category) {
        this.underlying = categoryReader;
        this.category = category;
    }

    public static SingleEventCategoryEventReader curryCategoryReader(EventSource source, String category) {
        return new SingleEventCategoryEventReader(source.readCategory(), category);
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        return underlying.readCategoryForwards(this.category, positionExclusive);
    }

    @Override
    public Position emptyStorePosition() {
        return underlying.emptyCategoryPosition(this.category);
    }
}
