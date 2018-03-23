package com.timgroup.eventstore.readerutils;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public final class FilteringEventReader implements EventReader {

    private final EventReader underlying;
    private final Predicate<? super ResolvedEvent> predicate;

    public FilteringEventReader(EventReader underlying, Predicate<? super ResolvedEvent> predicate) {
        this.underlying = requireNonNull(underlying);
        this.predicate = requireNonNull(predicate);
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        return underlying.readAllForwards(positionExclusive).filter(predicate);
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public Stream<ResolvedEvent> readAllBackwards() {
        return underlying.readAllBackwards().filter(predicate);
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public Stream<ResolvedEvent> readAllBackwards(Position positionExclusive) {
        return underlying.readAllBackwards(positionExclusive).filter(predicate);
    }

    @Nonnull
    @Override
    public Optional<ResolvedEvent> readLastEvent() {
        return underlying.readLastEvent().filter(predicate);
    }

    @Nonnull
    @Override
    public Position emptyStorePosition() {
        return underlying.emptyStorePosition();
    }

    @Override
    public String toString() {
        return "FilteringEventReader{" +
                "underlying=" + underlying +
                ", predicate=" + predicate +
                '}';
    }

    public static FilteringEventReader containingEventTypes(EventReader underlying, Set<String> eventTypes) {
        return new FilteringEventReader(underlying, e -> eventTypes.contains(e.eventRecord().eventType()));
    }

}
