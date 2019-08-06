package com.timgroup.eventstore.api;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public interface EventCategoryReader {
    @Nonnull
    @CheckReturnValue
    default Stream<ResolvedEvent> readCategoryForwards(String category) {
        return readCategoryForwards(category, emptyCategoryPosition(category));
    }

    @Nonnull
    @CheckReturnValue
    Stream<ResolvedEvent> readCategoryForwards(String category, Position positionExclusive);

    @Nonnull
    @CheckReturnValue
    default Stream<ResolvedEvent> readCategoriesForwards(List<String> categories, Position positionExclusive) {
        throw new UnsupportedOperationException("reading multiple categories forwards not supported yet");
    }

    @Nonnull
    @CheckReturnValue
    default Stream<ResolvedEvent> readCategoryBackwards(String category) {
        throw new UnsupportedOperationException("reading backwards is not yet supported");
    }

    @Nonnull
    @CheckReturnValue
    default Stream<ResolvedEvent> readCategoryBackwards(String category, Position positionExclusive) {
        throw new UnsupportedOperationException("reading backwards is not yet supported");
    }

    @Nonnull
    @CheckReturnValue
    default Optional<ResolvedEvent> readLastEventInCategory(String category) {
        return readCategoryBackwards(category).findFirst();
    }

    @Nonnull
    Position emptyCategoryPosition(String category);

    @Nonnull
    PositionCodec categoryPositionCodec(String category);
}
