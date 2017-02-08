package com.timgroup.eventstore.merging;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.collect.ImmutableList.copyOf;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

final class MergedEventReader<T extends Comparable<T>> implements EventReader {

    private final MergingStrategy<T> mergingStrategy;
    private final List<EventReader> readers;

    public MergedEventReader(MergingStrategy<T> mergingStrategy, EventReader... readers) {
        this.mergingStrategy = mergingStrategy;
        this.readers = copyOf(readers);
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        MergedEventReaderPosition mergedPosition = (MergedEventReaderPosition) positionExclusive;

        List<Stream<ResolvedEvent>> data = range(0, readers.size())
                .mapToObj(i -> readers.get(i)
                                      .readAllForwards(mergedPosition.inputPositions[i])
                                    //.takeWhile(re -> re.timestamp.isbefore(snapNow.minus(delay)));
                ).collect(toList());

        return StreamSupport.stream(new MergingSpliterator<>(mergingStrategy, mergedPosition, data), false)
                .onClose(() -> data.forEach(Stream::close));
    }

    @Override
    public Position emptyStorePosition() {
        Position[] positions = readers.stream().map(EventReader::emptyStorePosition).collect(toList()).toArray(new Position[0]);
        return new MergedEventReaderPosition(-1L, positions);
    }


}
