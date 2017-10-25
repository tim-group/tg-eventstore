package com.timgroup.eventstore.filesystem;

import java.util.stream.Stream;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;

final class FilteringCategoryReader implements EventCategoryReader {
    private final EventReader underlying;

    public FilteringCategoryReader(EventReader underlying) {
        this.underlying = underlying;
    }

    @Override
    public Stream<ResolvedEvent> readCategoryForwards(String category) {
        return underlying.readAllForwards().filter(re -> re.eventRecord().streamId().category().equals(category));
    }

    @Override
    public Stream<ResolvedEvent> readCategoryForwards(String category, Position positionExclusive) {
        return underlying.readAllForwards(positionExclusive).filter(re -> re.eventRecord().streamId().category().equals(category));
    }

    @Override
    public Stream<ResolvedEvent> readCategoryBackwards(String category) {
        return underlying.readAllBackwards().filter(re -> re.eventRecord().streamId().category().equals(category));
    }

    @Override
    public Stream<ResolvedEvent> readCategoryBackwards(String category, Position positionExclusive) {
        return underlying.readAllBackwards(positionExclusive).filter(re -> re.eventRecord().streamId().category().equals(category));
    }

    @Override
    public Position emptyCategoryPosition(String category) {
        return underlying.emptyStorePosition();
    }
}
