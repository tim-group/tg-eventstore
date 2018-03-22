package com.timgroup.eventstore.readerutils;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

@ParametersAreNonnullByDefault
public class TransformingEventReader implements EventReader {

    private final EventReader underlying;
    private final Function<ResolvedEvent, ResolvedEvent> transformer;

    protected TransformingEventReader(EventReader underlying, Function<ResolvedEvent, ResolvedEvent> transformer) {
        this.underlying = requireNonNull(underlying);
        this.transformer = requireNonNull(transformer);
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

    @CheckReturnValue
    @Nonnull
    @Override
    public final Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        return underlying.readAllForwards(positionExclusive).map(transformer);
    }

    @Nonnull
    @Override
    public final Position emptyStorePosition() {
        return underlying.emptyStorePosition();
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public final Stream<ResolvedEvent> readAllBackwards() {
        return underlying.readAllBackwards().map(transformer);
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public final Stream<ResolvedEvent> readAllBackwards(Position positionExclusive) {
        return underlying.readAllBackwards(positionExclusive).map(transformer);
    }

    @Nonnull
    @Override
    public Optional<ResolvedEvent> readLastEvent() {
        return underlying.readLastEvent().map(transformer);
    }

    @Override
    public String toString() {
        return "TransformingEventReader{" +
                "underlying=" + underlying +
                ", transformer=" + transformer +
                '}';
    }
}
