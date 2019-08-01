package com.timgroup.eventstore.merging;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

final class MergedEventReader implements EventReader {

    private final Clock clock;
    private final MergingStrategy<?> mergingStrategy;
    private final List<NamedReaderWithCodec> readers;
    private final PositionCodec positionCodec;

    MergedEventReader(Clock clock, MergingStrategy<?> mergingStrategy, NamedReaderWithCodec... readers) {
        checkArgument(
                Arrays.stream(readers).map(nr -> nr.name).distinct().count() == readers.length,
                "reader names must be unique"
        );

        this.clock = requireNonNull(clock);
        this.mergingStrategy = requireNonNull(mergingStrategy);
        this.readers = ImmutableList.copyOf(readers);
        this.positionCodec = MergedEventReaderPosition.codecFor(readers);
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        MergedEventReaderPosition mergedPosition = (MergedEventReaderPosition) positionExclusive;
        Instant snapTimestamp = clock.instant().minus(mergingStrategy.delay());

        List<Stream<ResolvedEvent>> data = range(0, readers.size())
                .mapToObj(i -> readers.get(i).reader.apply(mergedPosition.inputPositions[i])).collect(toList());

        List<Iterator<ResolvedEvent>> snappedData = data.stream()
                .map(eventStream -> takeWhileBefore(snapTimestamp, eventStream.iterator()))
                .collect(toList());

        return StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(new MergingIterator<>(mergingStrategy, mergedPosition, snappedData), ORDERED | NONNULL | DISTINCT),
                        false
                )
                .onClose(() -> data.forEach(Stream::close));
    }

    private static Iterator<ResolvedEvent> takeWhileBefore(Instant snapTimestamp, Iterator<ResolvedEvent> eventStream) {
        return new AbstractIterator<ResolvedEvent>() {
            @Override
            protected ResolvedEvent computeNext() {
                if (eventStream.hasNext()) {
                    ResolvedEvent event = eventStream.next();
                    if (!event.eventRecord().timestamp().isAfter(snapTimestamp)) {
                        return event;
                    }
                }
                return endOfData();
            }
        };
    }

    @Nonnull
    @Override
    public Position emptyStorePosition() {
        String[] names = readers.stream().map(r -> r.name).toArray(String[]::new);
        Position[] positions = readers.stream().map(r -> r.startingPosition).toArray(Position[]::new);
        return new MergedEventReaderPosition(names, positions);
    }

    @Nonnull
    @Override
    public PositionCodec positionCodec() {
        return positionCodec;
    }

    @Nonnull
    public PositionCodec positionCodecComparing(String eventSourceName) {
        int componentIndex = indexOf(eventSourceName);

        return PositionCodec.fromComparator(MergedEventReaderPosition.class,
                string -> (MergedEventReaderPosition) positionCodec.deserializePosition(string),
                positionCodec::serializePosition,
                comparing(pos -> pos.inputPositions[componentIndex], readers.get(componentIndex).codec::comparePositions)
        );
    }

    private int indexOf(String eventSourceName) {
        for (int i = 0; i < readers.size(); i++) {
            if (readers.get(i).name.equals(eventSourceName)) {
                return i;
            }
        }

        throw new IllegalArgumentException(format("Event source with name '%s' does not exist. Configured sources are: %s",
                eventSourceName,
                readers.stream().map(n -> "'" + n.name + "'").collect(joining(","))));
    }

    @Override
    public String toString() {
        return "MergedEventReader{" +
                "mergingStrategy.delay=" + mergingStrategy.delay() +
                ", readers=" + readers +
                '}';
    }
}
