package com.timgroup.eventstore.stitching;

import com.timgroup.eventstore.api.EventCategoryReader;
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
public final class BackfillStitchingEventCategoryReader implements EventCategoryReader {

    private final EventCategoryReader backfill;
    private final EventCategoryReader live;
    private final Position liveCutoffStartPosition;

    public BackfillStitchingEventCategoryReader(EventSource backfill, EventSource live, Position liveCutoffStartPosition) {
        this(backfill.readCategory(), live.readCategory(), liveCutoffStartPosition);
    }

    public BackfillStitchingEventCategoryReader(EventCategoryReader backfill, EventCategoryReader live, Position liveCutoffStartPosition) {
        this.backfill = backfill;
        this.live = requireNonNull(live);
        this.liveCutoffStartPosition = liveCutoffStartPosition;
    }

    @Nonnull
    @Override
    public PositionCodec categoryPositionCodec(String category) {
        return StitchedPosition.codec(backfill.categoryPositionCodec(category), live.categoryPositionCodec(category));
    }

    @Nonnull
    @CheckReturnValue
    @Override
    public Stream<ResolvedEvent> readCategoryForwards(String category, Position positionExclusive) {
        StitchedPosition stitchedPosition = (StitchedPosition) positionExclusive;
        if (stitchedPosition.isInBackfill(liveCutoffStartPosition)) {
            return BackfillStitchingEventForwardsSpliterator.stitchedStreamFrom(
                    backfill.readCategoryForwards(category, stitchedPosition.backfillPosition),
                    live.readCategoryForwards(category, stitchedPosition.livePosition),
                    stitchedPosition
            );
        } else {
            return BackfillStitchingEventForwardsSpliterator.stitchedStreamFrom(
                    Stream.empty(),
                    live.readCategoryForwards(category, stitchedPosition.livePosition),
                    stitchedPosition
            );
        }
    }

    @Nonnull
    @Override
    public StitchedPosition emptyCategoryPosition(String category) {
        return new StitchedPosition(backfill.emptyCategoryPosition(category), liveCutoffStartPosition);
    }

    @Override
    public String toString() {
        return "BackfillStitchingEventCategoryReader{" +
                "liveCutoffStartPosition=" + liveCutoffStartPosition +
                ", backfill=" + backfill +
                ", live=" + live +
                '}';
    }
}
