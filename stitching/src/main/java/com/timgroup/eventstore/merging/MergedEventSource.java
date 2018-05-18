package com.timgroup.eventstore.merging;

import com.google.common.collect.ImmutableList;
import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.tucker.info.Component;

import javax.annotation.Nonnull;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public final class MergedEventSource<T extends Comparable<T>> implements EventSource {

    private final MergedEventReader<T> eventReader;
    private final PositionCodec mergedEventReaderPositionCodec;
    private final NamedReaderWithCodec[] namedReaders;

    @SuppressWarnings("WeakerAccess")
    public MergedEventSource(Clock clock, MergingStrategy<T> mergingStrategy, NamedReaderWithCodec... namedReaders) {
        checkArgument(
            Stream.of(namedReaders).map(nr -> nr.name).distinct().count() == namedReaders.length,
            "reader names must be unique"
        );

        this.namedReaders = namedReaders;
        this.eventReader = new MergedEventReader<>(clock, mergingStrategy, this.namedReaders);
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
    public PositionCodec positionCodecComparing(String eventSourceName) {
        int componentIndex = indexOf(eventSourceName);

        return new PositionCodec() {
            @Override
            public Position deserializePosition(String string) {
                return mergedEventReaderPositionCodec.deserializePosition(string);
            }

            @Override
            public String serializePosition(Position position) {
                return mergedEventReaderPositionCodec.serializePosition(position);
            }

            @Override
            public int comparePositions(Position left, Position right) {
                MergedEventReaderPosition mergedLeft = (MergedEventReaderPosition) left;
                MergedEventReaderPosition mergedRight = (MergedEventReaderPosition) right;

                return namedReaders[componentIndex].codec.comparePositions(mergedLeft.inputPositions[componentIndex], mergedRight.inputPositions[componentIndex]);
            }
        };
    }

    private int indexOf(String eventSourceName) {
        for (int i = 0; i < namedReaders.length; i++) {
            if (namedReaders[i].name.equals(eventSourceName)) {
                return i;
            }
        }

        throw new IllegalArgumentException(format("Event source with name '%s' does not exist. Configured sources are: %s",
                eventSourceName,
                Stream.of(namedReaders).map(n -> "'" + n.name + "'").collect(Collectors.joining(","))));
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
