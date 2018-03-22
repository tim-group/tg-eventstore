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
import java.time.Instant;
import java.util.Collection;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;

public final class MergedEventSource<T extends Comparable<T>> implements EventSource {

    private final MergedEventReader<T> eventReader;
    private final PositionCodec mergedEventReaderPositionCodec;

    @SuppressWarnings("WeakerAccess")
    public MergedEventSource(Clock clock, MergingStrategy<T> mergingStrategy, NamedReaderWithCodec... namedReaders) {
        checkArgument(
            Stream.of(namedReaders).map(nr -> nr.name).distinct().count() == namedReaders.length,
            "reader names must be unique"
        );

        this.eventReader = new MergedEventReader<>(clock, mergingStrategy, namedReaders);
        this.mergedEventReaderPositionCodec = MergedEventReaderPosition.codecFor(namedReaders);
    }

    public static MergedEventSource<Instant> effectiveTimestampMergedEventSource(Clock clock, NamedReaderWithCodec... namedReaders) {
        return new MergedEventSource<>(clock, new MergingStrategy.EffectiveTimestampMergingStrategy(), namedReaders);
    }

    public static MergedEventSource<Instant> effectiveTimestampMergedEventSource(Clock clock, Duration delay, NamedReaderWithCodec... namedReaders) {
        return new MergedEventSource<>(clock, new MergingStrategy.EffectiveTimestampMergingStrategy().withDelay(delay), namedReaders);
    }

    public static MergedEventSource<Integer> streamOrderMergedEventSource(Clock clock, NamedReaderWithCodec... namedReaders) {
        return new MergedEventSource<>(clock, new MergingStrategy.StreamIndexMergingStrategy(), namedReaders);
    }

    public static MergedEventSource<Integer> streamOrderMergedEventSource(Clock clock, Duration delay, NamedReaderWithCodec... namedReaders) {
        return new MergedEventSource<>(clock, new MergingStrategy.StreamIndexMergingStrategy().withDelay(delay), namedReaders);
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
    public PositionCodec positionCodec() {
        return mergedEventReaderPositionCodec;
    }

    @Nonnull
    @Override
    public Collection<Component> monitoring() {
        return ImmutableList.of();
    }

    @Override
    public String toString() {
        return "MergedEventSource{" +
                "eventReader=" + eventReader +
                ", mergedEventReaderPositionCodec=" + mergedEventReaderPositionCodec +
                '}';
    }
}
