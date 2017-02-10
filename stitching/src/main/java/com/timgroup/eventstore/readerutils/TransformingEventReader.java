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

    protected TransformingEventReader(EventReader underlying, Function<ResolvedEvent, ResolvedEvent> transformer) {
        this.underlying = underlying;
        this.transformer = transformer;
    }

    public static TransformingEventReader transformEventRecords(EventReader underlying, Function<EventRecord, EventRecord> transformer) {
        return transformResolvedEvents(underlying, toResolvedEventTransformer(transformer));
    }

    public static TransformingEventReader transformResolvedEvents(EventReader underlying, Function<ResolvedEvent, ResolvedEvent> transformer) {
        return new TransformingEventReader(underlying, transformer);
    }


    public static Function<ResolvedEvent, ResolvedEvent> toResolvedEventTransformer(Function<EventRecord, EventRecord> transformer) {
        return re -> new ResolvedEvent(re.position(), transformer.apply(re.eventRecord()));
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
