package com.timgroup.eventstore.merging;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.time.Clock;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

final class MergedEventReader implements EventReader {

    private final Clock clock;
    private final MergingStrategy<?> mergingStrategy;
    private final List<NamedReaderWithCodec> readers;

    public MergedEventReader(Clock clock, MergingStrategy<?> mergingStrategy, NamedReaderWithCodec... readers) {
        this.clock = requireNonNull(clock);
        this.mergingStrategy = requireNonNull(mergingStrategy);
        this.readers = ImmutableList.copyOf(readers);
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

        return StreamSupport.stream(new MergingSpliterator<>(mergingStrategy, mergedPosition, snappedData), false)
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
        String[] names = readers.stream().map(r -> r.name).collect(toList()).toArray(new String[0]);
        Position[] positions = readers.stream().map(r -> r.startingPosition).collect(toList()).toArray(new Position[0]);
        return new MergedEventReaderPosition(names, positions);
    }

    @Override
    public String toString() {
        return "MergedEventReader{" +
                "mergingStrategy.delay=" + mergingStrategy.delay() +
                ", readers=" + readers +
                '}';
    }
}
