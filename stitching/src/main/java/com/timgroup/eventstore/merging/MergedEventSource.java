package com.timgroup.eventstore.merging;

import com.google.common.collect.ImmutableList;
import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.tucker.info.Component;

import javax.annotation.Nonnull;
import java.time.Clock;
import java.time.Duration;
import java.util.Collection;

public final class MergedEventSource implements EventSource {

    private final MergedEventReader eventReader;

    @SuppressWarnings("WeakerAccess")
    public MergedEventSource(Clock clock, MergingStrategy<?> mergingStrategy, NamedReaderWithCodec... namedReaders) {
        this.eventReader = new MergedEventReader(clock, mergingStrategy, namedReaders);
    }

    public static MergedEventSource effectiveTimestampMergedEventSource(Clock clock, NamedReaderWithCodec... namedReaders) {
        return new MergedEventSource(clock, new MergingStrategy.EffectiveTimestampMergingStrategy(), namedReaders);
    }

    public static MergedEventSource effectiveTimestampMergedEventSource(Clock clock, Duration delay, NamedReaderWithCodec... namedReaders) {
        return new MergedEventSource(clock, new MergingStrategy.EffectiveTimestampMergingStrategy().withDelay(delay), namedReaders);
    }

    public static MergedEventSource streamOrderMergedEventSource(Clock clock, NamedReaderWithCodec... namedReaders) {
        return new MergedEventSource(clock, new MergingStrategy.StreamIndexMergingStrategy(), namedReaders);
    }

    public static MergedEventSource streamOrderMergedEventSource(Clock clock, Duration delay, NamedReaderWithCodec... namedReaders) {
        return new MergedEventSource(clock, new MergingStrategy.StreamIndexMergingStrategy().withDelay(delay), namedReaders);
    }

    @Nonnull
    @Override
    public EventReader readAll() {
        return eventReader;
    }

    @Nonnull
    @Override
    public EventCategoryReader readCategory() {
        throw new UnsupportedOperationException("category reading unsupported on merged stream");
    }

    @Nonnull
    @Override
    public EventStreamReader readStream() {
        throw new UnsupportedOperationException("stream reading unsupported on merged stream");
    }

    @Nonnull
    @Override
    public EventStreamWriter writeStream() {
        throw new UnsupportedOperationException("writing unsupported on merged stream");
    }

    @Nonnull
    @Override
    @Deprecated
    public PositionCodec positionCodec() {
        return eventReader.positionCodec();
    }

    @Nonnull
    public PositionCodec positionCodecComparing(String eventSourceName) {
        return eventReader.positionCodecComparing(eventSourceName);
    }

    @Nonnull
    @Override
    public Collection<Component> monitoring() {
        return ImmutableList.of();
    }

    @Override
    public String toString() {
        return "MergedEventSource{" + eventReader + "}";
    }
}
