package com.timgroup.eventstore.readerutils;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class TransformingEventReader implements EventReader {

    private final EventReader underlying;
    private final UnaryOperator<ResolvedEvent> transformer;

    protected TransformingEventReader(EventReader underlying, UnaryOperator<ResolvedEvent> transformer) {
        this.underlying = requireNonNull(underlying);
        this.transformer = requireNonNull(transformer);
    }

    public static TransformingEventReader transformEventRecords(EventReader underlying, UnaryOperator<EventRecord> transformer) {
        return transformResolvedEvents(underlying, toResolvedEventTransformer(transformer));
    }

    public static TransformingEventReader transformResolvedEvents(EventReader underlying, UnaryOperator<ResolvedEvent> transformer) {
        return new TransformingEventReader(underlying, transformer);
    }


    public static UnaryOperator<ResolvedEvent> toResolvedEventTransformer(UnaryOperator<EventRecord> transformer) {
        requireNonNull(transformer);
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

    @Nonnull
    @Override
    public PositionCodec storePositionCodec() {
        return underlying.storePositionCodec();
    }

    @Override
    public String toString() {
        return "TransformingEventReader{" +
                "underlying=" + underlying +
                ", transformer=" + transformer +
                '}';
    }
}
