package com.timgroup.eventstore;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

public final class FilteringEventReader implements EventReader {

    private final EventReader underlying;
    private final Predicate<? super ResolvedEvent> predicate;

    public FilteringEventReader(EventReader underlying, Predicate<? super ResolvedEvent> predicate) {
        this.underlying = underlying;
        this.predicate = predicate;
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        return underlying.readAllForwards(positionExclusive).filter(predicate);
    }

    @Override
    public Position emptyStorePosition() {
        return underlying.emptyStorePosition();
    }

    public static FilteringEventReader containingEventTypes(EventReader underlying, Set<String> eventTypes) {
        return new FilteringEventReader(underlying, e -> eventTypes.contains(e.eventRecord().eventType()));
    }

}
