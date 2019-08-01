package com.timgroup.eventstore.filesystem;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

final class FilteringCategoryReader implements EventCategoryReader {
    private final EventReader underlying;

    public FilteringCategoryReader(EventReader underlying) {
        this.underlying = requireNonNull(underlying);
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public Stream<ResolvedEvent> readCategoryForwards(String category) {
        return underlying.readAllForwards().filter(re -> re.eventRecord().streamId().category().equals(category));
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public Stream<ResolvedEvent> readCategoryForwards(String category, Position positionExclusive) {
        return underlying.readAllForwards(positionExclusive).filter(re -> re.eventRecord().streamId().category().equals(category));
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public Stream<ResolvedEvent> readCategoryBackwards(String category) {
        return underlying.readAllBackwards().filter(re -> re.eventRecord().streamId().category().equals(category));
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public Stream<ResolvedEvent> readCategoryBackwards(String category, Position positionExclusive) {
        return underlying.readAllBackwards(positionExclusive).filter(re -> re.eventRecord().streamId().category().equals(category));
    }

    @Nonnull
    @Override
    public Position emptyCategoryPosition(String category) {
        return underlying.emptyStorePosition();
    }

    @Nonnull
    @Override
    public PositionCodec categoryPositionCodec() {
        return underlying.storePositionCodec();
    }

    @Override
    public String toString() {
        return "FilteringCategoryReader{" +
                "underlying=" + underlying +
                '}';
    }
}
