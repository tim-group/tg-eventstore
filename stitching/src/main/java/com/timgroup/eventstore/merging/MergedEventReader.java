package com.timgroup.eventstore.merging;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;

import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static java.lang.Long.MAX_VALUE;
import static java.util.stream.Collectors.toList;

public final class MergedEventReader implements EventReader {
    private EventReader[] readers;

    public MergedEventReader(EventReader... readers) {
        this.readers = readers;
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        return StreamSupport.stream(new MergingSpliterator((MergedEventReaderPosition) positionExclusive, readers), false);
    }

    private static final class MergingSpliterator implements Spliterator<ResolvedEvent> {

        private final EventReader[] readers;

        private MergedEventReaderPosition currentPosition;

        private MergingSpliterator(MergedEventReaderPosition startPosition, EventReader... readers) {
            this.readers = readers;
            currentPosition = startPosition;
        }

        @Override
        public boolean tryAdvance(Consumer<? super ResolvedEvent> consumer) {
            for (int readerIndex = 0; readerIndex < readers.length; readerIndex ++) {
                EventReader reader = readers[readerIndex];
                Optional<ResolvedEvent> maybeEvent = reader.readAllForwards(currentPosition.inputPositions[readerIndex]).findFirst();
                if (maybeEvent.isPresent()) {
                    ResolvedEvent re = maybeEvent.get();
                    EventRecord record = re.eventRecord();
                    currentPosition = currentPosition.withNextPosition(readerIndex, re.position());
                    consumer.accept(new ResolvedEvent(
                            currentPosition,
                            eventRecord(
                                record.timestamp(),
                                StreamId.streamId("input", "all"),
                                currentPosition.outputEventNumber,
                                record.eventType(),
                                record.data(),
                                record.metadata()
                            )
                    ));
                    return true;
                }
            }
            return false;
        }

        @Override
        public Spliterator<ResolvedEvent> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return MAX_VALUE;
        }

        @Override
        public int characteristics() {
            return ORDERED | NONNULL | DISTINCT;
        }
    }


    @Override
    public Position emptyStorePosition() {
        Position[] positions = Stream.of(readers).map(EventReader::emptyStorePosition).collect(toList()).toArray(new Position[0]);
        return new MergedEventReaderPosition(-1L, positions);
    }

    private static class MergedEventReaderPosition implements Position {
        private final long outputEventNumber;
        private final Position[] inputPositions;

        MergedEventReaderPosition(long outputEventNumber, Position... inputPositions) {
            this.outputEventNumber = outputEventNumber;
            this.inputPositions = inputPositions;
        }

        public MergedEventReaderPosition withNextPosition(int readerIndex, Position position) {
            Position[] newPositions = inputPositions.clone();
            newPositions[readerIndex] = position;
            return new MergedEventReaderPosition(outputEventNumber + 1L, newPositions);
        }
    }

    public static class MergedEventReaderPositionCodec implements PositionCodec {
        @Override
        public Position deserializePosition(String string) {
            return new MergedEventReaderPosition(-1L);
        }

        @Override
        public String serializePosition(Position position) {
            return "";
        }
    }
}
