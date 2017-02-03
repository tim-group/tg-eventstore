package com.timgroup.eventstore.stitching;

import com.timgroup.eventstore.api.*;
import com.timgroup.eventstore.stitching.StitchedPosition.StitchedPositionCodec;
import com.timgroup.tucker.info.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class BackfillStitchingEventSource implements EventSource {

    private final EventSource backfill;
    private final EventSource live;
    private final StitchedPositionCodec positionCodec;
    private final EventReader eventReader;

    public BackfillStitchingEventSource(EventSource backfill, EventSource live, Position liveCutoffStartPosition) {
        this.backfill = backfill;
        this.live = live;
        this.positionCodec = new StitchedPositionCodec(backfill.positionCodec(), live.positionCodec());
        this.eventReader = new BackfillStitchingEventReader(backfill, live, liveCutoffStartPosition);
    }

    @Override
    public EventReader readAll() {
        return this.eventReader;
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
