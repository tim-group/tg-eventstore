package com.timgroup.eventstore.merging;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.merging.MergedEventReaderPosition.MergedEventReaderPositionCodec;
import com.timgroup.tucker.info.Component;

import java.time.Instant;
import java.util.Collection;

public final class MergedEventSource<T extends Comparable<T>> implements EventSource {

    private final MergedEventReader<T> eventReader;
    private final MergedEventReaderPositionCodec mergedEventReaderPositionCodec;

    @SuppressWarnings("WeakerAccess")
    public MergedEventSource(MergingStrategy<T> mergingStrategy, EventReader... readers) {
        this.eventReader = new MergedEventReader<>(mergingStrategy, readers);
        this.mergedEventReaderPositionCodec = new MergedEventReaderPositionCodec();
    }

    public static MergedEventSource<Instant> effectiveTimestampMergedEventSource(EventReader... readers) {
        return new MergedEventSource<>(new MergingStrategy.EffectiveTimestampMergingStrategy(), readers);
    }

    public static MergedEventSource<Integer> streamOrderMergedEventSource(EventReader... readers) {
        return new MergedEventSource<>(new MergingStrategy.StreamIndexMergingStrategy(), readers);
    }

    @Override
    public EventReader readAll() {
        return eventReader;
    }

    @Override
    public EventCategoryReader readCategory() {
        throw new UnsupportedOperationException("category reading unsupported on merged stream");
    }

    @Override
    public EventStreamReader readStream() {
        throw new UnsupportedOperationException("stream reading unsupported on merged stream");
    }

    @Override
    public EventStreamWriter writeStream() {
        throw new UnsupportedOperationException("writing unsupported on merged stream");
    }

    @Override
    public PositionCodec positionCodec() {
        return mergedEventReaderPositionCodec;
    }

    @Override
    public Collection<Component> monitoring() {
        return null;
    }


}
