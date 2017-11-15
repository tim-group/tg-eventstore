package com.timgroup.eventstore.datastream;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.stream.Stream;

public class DataStreamEventReader implements EventReader {
    private final EventReader underlying;

    public DataStreamEventReader(EventReader underlying) {
        this.underlying = underlying;
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards() {
        return underlying.readAllForwards();
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        return null;
    }

    @Override
    public Position emptyStorePosition() {
        return null;
    }
}
