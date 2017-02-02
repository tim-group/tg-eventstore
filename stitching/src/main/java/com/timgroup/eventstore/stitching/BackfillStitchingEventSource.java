package com.timgroup.eventstore.stitching;

import com.timgroup.eventstore.api.*;
import com.timgroup.eventstore.stitching.StitchedPosition.StitchedPositionCodec;
import com.timgroup.tucker.info.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public final class BackfillStitchingEventSource implements EventSource {

    private final EventSource backfill;
    private final EventSource live;
    private final StitchedPositionCodec positionCodec;
    private final StitchedPosition emptyStorePosition;

    public BackfillStitchingEventSource(EventSource backfill, EventSource live, Position liveCutoffStartPosition) {
        this.backfill = backfill;
        this.live = live;
        this.positionCodec = new StitchedPositionCodec(backfill.positionCodec(), live.positionCodec());
        this.emptyStorePosition = new StitchedPosition(backfill.readAll().emptyStorePosition(), liveCutoffStartPosition);
    }

    @Override
    public EventReader readAll() {
        return new EventReader() {
            @Override
            public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
                return Stream.empty();
            }

            @Override
            public Position emptyStorePosition() {
                return emptyStorePosition;
            }
        };
    }

    @Override
    public EventCategoryReader readCategory() {
        throw new UnsupportedOperationException("stitched store does not support reading categories");
    }

    @Override
    public EventStreamReader readStream() {
        throw new UnsupportedOperationException("stitched store does not support reading streams");
    }

    @Override
    public EventStreamWriter writeStream() {
        throw new UnsupportedOperationException("stitched store does not support writing");
    }

    @Override
    public PositionCodec positionCodec() {
        return positionCodec;
    }

    @Override
    public Collection<Component> monitoring() {
        List<Component> result = new ArrayList<>();
        result.addAll(backfill.monitoring());
        result.addAll(live.monitoring());
        return result;
    }


}
