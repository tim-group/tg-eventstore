package com.timgroup.eventstore.merging;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public final class MergedEventReader implements EventReader {
    private EventReader[] readers;

    public MergedEventReader(EventReader... readers) {
        this.readers = readers;
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        MergedEventReaderPosition mergedPositionExclusion = (MergedEventReaderPosition) positionExclusive;

        Iterator<Position> positions = Arrays.asList(mergedPositionExclusion.positions).iterator();
        return Stream.of(readers).flatMap(r -> r.readAllForwards(positions.next()));
    }

    @Override
    public Position emptyStorePosition() {
        Position[] positions = Stream.of(readers).map(r -> r.emptyStorePosition()).collect(toList()).toArray(new Position[0]);
        return new MergedEventReaderPosition(positions);
    }

    private static class MergedEventReaderPosition implements Position {
        final private Position[] positions;

        MergedEventReaderPosition(Position... positions) {
            this.positions = positions;
        }
    }
}
