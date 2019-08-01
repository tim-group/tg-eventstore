package com.timgroup.eventstore.stitching;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

@ParametersAreNonnullByDefault
public final class BackfillStitchingEventReader implements EventReader, EventCategoryReader {

    private final StitchedPosition emptyStorePosition;
    private final EventSource backfill;
    private final EventSource live;

    public BackfillStitchingEventReader(EventSource backfill, EventSource live, Position liveCutoffStartPosition) {
        this.backfill = backfill;
        this.live = requireNonNull(live);
        this.emptyStorePosition = new StitchedPosition(backfill.readAll().emptyStorePosition(), liveCutoffStartPosition);
    }

    @Nonnull
    @Override
    public Position emptyStorePosition() {
        return emptyStorePosition;
    }

    @Nonnull
    @Override
    public PositionCodec positionCodec() {
        return StitchedPosition.codec(backfill.readAll().positionCodec(), live.readAll().positionCodec());
    }

    @Nonnull
    @Override
    public PositionCodec categoryPositionCodec() {
        return StitchedPosition.codec(backfill.readCategory().categoryPositionCodec(), live.readCategory().categoryPositionCodec());
    }

    @Nonnull
    @CheckReturnValue
    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        StitchedPosition stitchedPosition = (StitchedPosition) positionExclusive;
        if (stitchedPosition.isInBackfill(emptyStorePosition)) {
            return BackfillStitchingEventForwardsSpliterator.stitchedStreamFrom(
                    backfill.readAll().readAllForwards(stitchedPosition.backfillPosition),
                    live.readAll().readAllForwards(stitchedPosition.livePosition),
                    stitchedPosition
            );
        } else {
            return BackfillStitchingEventForwardsSpliterator.stitchedStreamFrom(
                    Stream.empty(),
                    live.readAll().readAllForwards(stitchedPosition.livePosition),
                    stitchedPosition
            );
        }
    }

    @Nonnull
    @CheckReturnValue
    @Override
    public Stream<ResolvedEvent> readCategoryForwards(String category, Position positionExclusive) {
        StitchedPosition stitchedPosition = (StitchedPosition) positionExclusive;
        if (stitchedPosition.isInBackfill(emptyStorePosition)) {
            return BackfillStitchingEventForwardsSpliterator.stitchedStreamFrom(
                    backfill.readCategory().readCategoryForwards(category, stitchedPosition.backfillPosition),
                    live.readCategory().readCategoryForwards(category, stitchedPosition.livePosition),
                    stitchedPosition
            );
        } else {
            return BackfillStitchingEventForwardsSpliterator.stitchedStreamFrom(
                    Stream.empty(),
                    live.readCategory().readCategoryForwards(category, stitchedPosition.livePosition),
                    stitchedPosition
            );
        }
    }

    @Nonnull
    @Override
    public Position emptyCategoryPosition(String category) {
        return emptyStorePosition;
    }

    @Override
    public String toString() {
        return "BackfillStitchingEventReader{" +
                "emptyStorePosition=" + emptyStorePosition +
                ", backfill=" + backfill +
                ", live=" + live +
                '}';
    }
}
