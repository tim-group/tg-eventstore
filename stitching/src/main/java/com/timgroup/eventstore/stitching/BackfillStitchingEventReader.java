package com.timgroup.eventstore.stitching;

import com.timgroup.eventstore.api.*;

import java.util.stream.Stream;

public final class BackfillStitchingEventReader implements EventReader, EventCategoryReader {

    private final StitchedPosition emptyStorePosition;
    private final EventSource backfill;
    private final EventSource live;

    public BackfillStitchingEventReader(EventSource backfill, EventSource live, Position liveCutoffStartPosition) {
        this.backfill = backfill;
        this.live = live;
        this.emptyStorePosition = new StitchedPosition(backfill.readAll().emptyStorePosition(), liveCutoffStartPosition);
    }

    @Override
    public Position emptyStorePosition() {
        return emptyStorePosition;
    }

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

    @Override
    public Position emptyCategoryPosition(String category) {
        return emptyStorePosition;
    }
}
