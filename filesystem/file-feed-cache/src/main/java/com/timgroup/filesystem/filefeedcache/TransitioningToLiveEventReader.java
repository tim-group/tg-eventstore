package com.timgroup.filesystem.filefeedcache;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;

import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class TransitioningToLiveEventReader implements EventReader {

    private final EventReader backfill;
    private final EventReader live;
    private final Position liveCutoffStartPosition;

    public TransitioningToLiveEventReader(EventSource backfill, EventSource live, Position liveCutoffStartPosition) {
        this(backfill.readAll(), live.readAll(), liveCutoffStartPosition);
    }

    public TransitioningToLiveEventReader(EventReader backfill, EventReader live, Position liveCutoffStartPosition) {
        this.backfill = backfill;
        this.live = requireNonNull(live);
        this.liveCutoffStartPosition = liveCutoffStartPosition;
    }

    @Nonnull
    @Override
    public TransitionPosition emptyStorePosition() {
        return new TransitionPosition(backfill.emptyStorePosition());
    }

    @Nonnull
    @Override
    public PositionCodec storePositionCodec() {
        return TransitionPosition.codec(backfill.storePositionCodec(), live.storePositionCodec(), Long.valueOf(liveCutoffStartPosition.toString()));
    }

    @Nonnull
    @CheckReturnValue
    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        TransitionPosition stitchedPosition = (TransitionPosition) positionExclusive;
        return TransitioningEventsForwardsSpliterator.transitionedStreamFrom(
                backfill.readAllForwards(stitchedPosition.position),
                live.readAllForwards(liveCutoffStartPosition),
                stitchedPosition);
    }

    @Override
    public String toString() {
        return "AppendingEventReader{" +
                "backfill=" + backfill +
                ", live=" + live +
                ", liveCutoffStartPosition=" + liveCutoffStartPosition +
                '}';
    }
}
