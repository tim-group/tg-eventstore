package com.timgroup.eventstore.merging;

import com.timgroup.eventstore.api.*;
import com.timgroup.eventstore.merging.MergedEventReaderPosition.MergedEventReaderPositionCodec;
import com.timgroup.tucker.info.Component;

import java.time.Instant;
import java.util.Collection;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;

public final class MergedEventSource<T extends Comparable<T>> implements EventSource {

    private final MergedEventReader<T> eventReader;
    private final MergedEventReaderPositionCodec mergedEventReaderPositionCodec;

    @SuppressWarnings("WeakerAccess")
    public MergedEventSource(MergingStrategy<T> mergingStrategy, NamedReaderWithCodec... namedReaders) {
        checkArgument(
            Stream.of(namedReaders).map(nr -> nr.name).distinct().count() == namedReaders.length,
            "reader names must be unique"
        );

        this.eventReader = new MergedEventReader<>(
                mergingStrategy,
                Stream.of(namedReaders).map(NamedReaderWithCodec::toReader).toArray(EventReader[]::new)
        );
        this.mergedEventReaderPositionCodec = new MergedEventReaderPositionCodec(namedReaders);
    }

    public static MergedEventSource<Instant> effectiveTimestampMergedEventSource(NamedReaderWithCodec... namedReaders) {
        return new MergedEventSource<>(new MergingStrategy.EffectiveTimestampMergingStrategy(), namedReaders);
    }

    public static MergedEventSource<Integer> streamOrderMergedEventSource(NamedReaderWithCodec... namedReaders) {
        return new MergedEventSource<>(new MergingStrategy.StreamIndexMergingStrategy(), namedReaders);
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
