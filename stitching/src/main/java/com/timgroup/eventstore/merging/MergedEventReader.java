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
    private final List<NamedReaderWithCodec> readers;

    public MergedEventReader(Clock clock, MergingStrategy<T> mergingStrategy, NamedReaderWithCodec... readers) {
        this.clock = clock;
        this.mergingStrategy = mergingStrategy;
        this.readers = copyOf(readers);
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        MergedEventReaderPosition mergedPosition = (MergedEventReaderPosition) positionExclusive;
        Instant snapTimestamp = clock.instant().minus(mergingStrategy.delay());

        List<Stream<ResolvedEvent>> data = range(0, readers.size())
                .mapToObj(i -> readers.get(i).reader.readAllForwards(mergedPosition.inputPositions[i])).collect(toList());

        @SuppressWarnings("ConstantConditions")
        List<Iterator<ResolvedEvent>> snappedData = data.stream().map(eventStream ->
                filter(eventStream.iterator(), er ->
                        !er.eventRecord().timestamp().isAfter(snapTimestamp)
                )).collect(toList());

        return StreamSupport.stream(new MergingSpliterator<>(mergingStrategy, mergedPosition, snappedData), false)
                .onClose(() -> data.forEach(Stream::close));
    }

    @Override
    public Position emptyStorePosition() {
        String[] names = readers.stream().map(r -> r.name).collect(toList()).toArray(new String[0]);
        Position[] positions = readers.stream().map(r -> r.startingPosition).collect(toList()).toArray(new Position[0]);
        return new MergedEventReaderPosition(names, positions);
    }


}
