package com.timgroup.eventstore.filesystem;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.NoSuchStreamException;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

final class FilteringStreamReader implements EventStreamReader {
    private final EventReader underlying;
    private final Predicate<? super StreamId> streamExistsPredicate;

    public FilteringStreamReader(EventReader underlying, Predicate<? super StreamId> streamExistsPredicate) {
        this.underlying = requireNonNull(underlying);
        this.streamExistsPredicate = requireNonNull(streamExistsPredicate);
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public Stream<ResolvedEvent> readStreamForwards(StreamId streamId, long eventNumber) {
        if (!streamExistsPredicate.test(streamId)) throw new NoSuchStreamException(streamId);
        return underlying.readAllForwards()
                .filter(re -> re.eventRecord().streamId().equals(streamId) && re.eventRecord().eventNumber() > eventNumber);
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public Stream<ResolvedEvent> readStreamBackwards(StreamId streamId) {
        if (!streamExistsPredicate.test(streamId)) throw new NoSuchStreamException(streamId);
        return underlying.readAllBackwards()
                .filter(re -> re.eventRecord().streamId().equals(streamId));
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public Stream<ResolvedEvent> readStreamBackwards(StreamId streamId, long eventNumber) {
        if (!streamExistsPredicate.test(streamId)) throw new NoSuchStreamException(streamId);
        return underlying.readAllBackwards()
                .filter(re -> re.eventRecord().streamId().equals(streamId) && re.eventRecord().eventNumber() < eventNumber);
    }

    @Nonnull
    @Override
    public PositionCodec streamPositionCodec() {
        return underlying.storePositionCodec();
    }

    @Override
    public String toString() {
        return "FilteringStreamReader{" +
                "underlying=" + underlying +
                ", streamExistsPredicate=" + streamExistsPredicate +
                '}';
    }
}
