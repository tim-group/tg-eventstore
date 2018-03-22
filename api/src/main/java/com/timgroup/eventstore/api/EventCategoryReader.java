package com.timgroup.eventstore.api;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Optional;
import java.util.stream.Stream;

@ParametersAreNonnullByDefault
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
}
