package com.timgroup.eventstore.merging;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.time.Clock;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Iterators.filter;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

final class MergedEventReader<T extends Comparable<T>> implements EventReader {

    private final Clock clock;
    private final MergingStrategy<T> mergingStrategy;
    private final List<EventReader> readers;

    public MergedEventReader(Clock clock, MergingStrategy<T> mergingStrategy, EventReader... readers) {
        this.clock = clock;
        this.mergingStrategy = mergingStrategy;
        this.readers = copyOf(readers);
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        MergedEventReaderPosition mergedPosition = (MergedEventReaderPosition) positionExclusive;
        Instant snapTimestamp = clock.instant().minus(mergingStrategy.delay());

        Stream<Stream<ResolvedEvent>> data = range(0, readers.size())
                .mapToObj(i -> readers.get(i).readAllForwards(mergedPosition.inputPositions[i]));

        @SuppressWarnings("ConstantConditions")
        List<Iterator<ResolvedEvent>> snappedData = data.map(eventStream ->
                filter(eventStream.iterator(), er -> er.eventRecord().timestamp().isBefore(snapTimestamp))).collect(toList());

        return StreamSupport.stream(new MergingSpliterator<>(mergingStrategy, mergedPosition, snappedData), false)
                .onClose(() -> data.forEach(Stream::close));
    }

    @Override
    public Position emptyStorePosition() {
        Position[] positions = readers.stream().map(EventReader::emptyStorePosition).collect(toList()).toArray(new Position[0]);
        return new MergedEventReaderPosition(-1L, positions);
    }


}
