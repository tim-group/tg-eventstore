package com.timgroup.eventstore.merging;

import com.timgroup.eventstore.api.*;

import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Stream;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
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
        return Stream.of(readers).flatMap(r -> r.readAllForwards(positions.next())).map(re -> {
            EventRecord record = re.eventRecord();
            return new ResolvedEvent(re.position(), eventRecord(
                    record.timestamp(),
                    StreamId.streamId("input", "all"),
                    record.eventNumber(),
                    record.eventType(),
                    record.data(),
                    record.metadata()
            ));
        });
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

    public static class MergedEventReaderPositionCodec implements PositionCodec {
        @Override
        public Position deserializePosition(String string) {
            return new MergedEventReaderPosition();
        }

        @Override
        public String serializePosition(Position position) {
            return "";
        }
    }
}
