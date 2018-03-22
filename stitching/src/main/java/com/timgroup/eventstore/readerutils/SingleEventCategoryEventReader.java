package com.timgroup.eventstore.readerutils;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

@ParametersAreNonnullByDefault
public final class SingleEventCategoryEventReader implements EventReader {

    private final EventCategoryReader underlying;
    private final String category;

    public SingleEventCategoryEventReader(EventCategoryReader categoryReader, String category) {
        this.underlying = requireNonNull(categoryReader);
        this.category = requireNonNull(category);
    }

    public static SingleEventCategoryEventReader curryCategoryReader(EventSource source, String category) {
        return new SingleEventCategoryEventReader(source.readCategory(), category);
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        return underlying.readCategoryForwards(this.category, positionExclusive);
    }

    @Nonnull
    @Override
    public Position emptyStorePosition() {
        return underlying.emptyCategoryPosition(this.category);
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public Stream<ResolvedEvent> readAllBackwards() {
        return underlying.readCategoryBackwards(this.category);
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public Stream<ResolvedEvent> readAllBackwards(Position positionExclusive) {
        return underlying.readCategoryBackwards(this.category, positionExclusive);
    }

    @Nonnull
    @Override
    public Optional<ResolvedEvent> readLastEvent() {
        return underlying.readLastEventInCategory(this.category);
    }

    @Override
    public String toString() {
        return "SingleEventCategoryEventReader{" +
                "underlying=" + underlying +
                ", category='" + category + '\'' +
                '}';
    }
}
