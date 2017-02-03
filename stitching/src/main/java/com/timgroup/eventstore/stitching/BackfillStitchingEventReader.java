package com.timgroup.eventstore.stitching;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.stream.Stream;

public final class BackfillStitchingEventReader implements EventReader {

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
}
