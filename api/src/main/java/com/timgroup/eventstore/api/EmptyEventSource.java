package com.timgroup.eventstore.api;

import com.timgroup.tucker.info.Component;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Stream;

public final class EmptyEventSource implements EventSource, EventReader, EventCategoryReader, EventStreamReader {
    @Nonnull
    public static final EmptyEventSource INSTANCE = new EmptyEventSource();

    public static final class EmptyPosition implements Position {
        public static final EmptyPosition INSTANCE = new EmptyPosition();

        public static final PositionCodec CODEC = PositionCodec.fromComparator(
                EmptyPosition.class,
                ignored -> INSTANCE,
                pos -> "Empty",
                (EmptyPosition a, EmptyPosition b) -> 0
        );

        @Override
        public boolean equals(Object obj) {
            return obj != null && obj.getClass() == EmptyPosition.class;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        public String toString() {
            return "EmptyPosition";
        }
    }

    private EmptyEventSource() {
    }

    @Nonnull
    @Override
    public EventReader readAll() {
        return this;
    }

    @Nonnull
    @Override
    public EventCategoryReader readCategory() {
        return this;
    }

    @Nonnull
    @Override
    public EventStreamReader readStream() {
        return this;
    }

    @Nonnull
    @Override
    public EventStreamWriter writeStream() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public Collection<Component> monitoring() {
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public Stream<ResolvedEvent> readCategoryForwards(String category, Position positionExclusive) {
        return Stream.empty();
    }

    @Nonnull
    @Override
    public Position emptyCategoryPosition(String category) {
        return EmptyPosition.INSTANCE;
    }

    @Nonnull
    @Override
    public PositionCodec categoryPositionCodec(String category) {
        return EmptyPosition.CODEC;
    }

    @Nonnull
    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        return Stream.empty();
    }

    @Nonnull
    @Override
    public Position emptyStorePosition() {
        return EmptyPosition.INSTANCE;
    }

    @Nonnull
    @Override
    public PositionCodec storePositionCodec() {
        return EmptyPosition.CODEC;
    }

    @Nonnull
    @Override
    public Stream<ResolvedEvent> readStreamForwards(StreamId streamId, long eventNumber) {
        return Stream.empty();
    }

    @Nonnull
    @Override
    public PositionCodec streamPositionCodec() {
        return EmptyPosition.CODEC;
    }
}
