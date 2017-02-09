package com.timgroup.eventstore.readerutils;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.function.Function;
import java.util.stream.Stream;

public class TransformingEventReader implements EventReader {

    private final EventReader underlying;
    private final Function<ResolvedEvent, ResolvedEvent> transformer;

    public TransformingEventReader(EventReader underlying, Function<EventRecord, EventRecord> transformer) {
        this.underlying = underlying;
        this.transformer = re -> new ResolvedEvent(re.position(), transformer.apply(re.eventRecord()));
    }

    public static TransformingEventReader transform(EventReader underlying, Function<EventRecord, EventRecord> transformer) {
        return new TransformingEventReader(underlying, transformer);
    }

    @Override
    public final Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        return underlying.readAllForwards(positionExclusive).map(transformer);
    }

    @Override
    public final Position emptyStorePosition() {
        return underlying.emptyStorePosition();
    }

    @Override
    public final Stream<ResolvedEvent> readAllBackwards() {
        return underlying.readAllBackwards().map(transformer);
    }

    @Override
    public final Stream<ResolvedEvent> readAllBackwards(Position positionExclusive) {
        return underlying.readAllBackwards(positionExclusive).map(transformer);
    }
}
