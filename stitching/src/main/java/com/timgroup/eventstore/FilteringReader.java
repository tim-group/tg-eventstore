package com.timgroup.eventstore;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class FilteringReader implements EventReader {

    private final EventReader underlying;
    private Predicate<? super ResolvedEvent> predicate;

    public FilteringReader(EventReader underlying, Predicate<? super ResolvedEvent> predicate) {
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

    public static FilteringReader containingEventTypes(EventReader underlying, Set<String> eventTypes) {
        return new FilteringReader(underlying, (e) -> eventTypes.contains(e.eventRecord().eventType()));
    }

}
