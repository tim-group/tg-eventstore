package com.timgroup.eventstore.merging;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.Position;
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

final class MergedEventReader<T extends Comparable<T>> implements EventReader {

    private final MergingStrategy<T> mergingStrategy;
    private final EventReader[] readers;

    public MergedEventReader(MergingStrategy<T> mergingStrategy, EventReader... readers) {
        this.mergingStrategy = mergingStrategy;
        this.readers = readers;
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        return StreamSupport.stream(new MergingSpliterator<>(mergingStrategy, (MergedEventReaderPosition) positionExclusive, readers), false);
    }

    private static final class MergingSpliterator<T extends Comparable<T>> implements Spliterator<ResolvedEvent> {

        private final MergingStrategy<T> mergingStrategy;
        private final EventReader[] readers;

        private MergedEventReaderPosition currentPosition;

        private MergingSpliterator(MergingStrategy<T> mergingStrategy, MergedEventReaderPosition startPosition, EventReader... readers) {
            this.mergingStrategy = mergingStrategy;
            this.readers = readers;
            this.currentPosition = startPosition;
        }

        @Override
        public boolean tryAdvance(Consumer<? super ResolvedEvent> consumer) {
            Optional<ResolvedEvent> maybeNextInputEvent = advanceToNextEvent();
            if (maybeNextInputEvent.isPresent()) {
                EventRecord record = maybeNextInputEvent.get().eventRecord();
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
            return false;
        }

        private Optional<ResolvedEvent> advanceToNextEvent() {
            Optional<T> candidateOrderingValue = Optional.empty();
            Optional<ResolvedEvent> candidate = Optional.empty();
            int candidateIndex = -1;

            for (int readerIndex = 0; readerIndex < readers.length; readerIndex++) {
                EventReader reader = readers[readerIndex];
                Optional<ResolvedEvent> maybeEvent = reader.readAllForwards(currentPosition.inputPositions[readerIndex]).findFirst();
                if (maybeEvent.isPresent()) {
                    T orderingValue = mergingStrategy.toComparable(maybeEvent.get());
                    if (!candidateOrderingValue.isPresent() || orderingValue.compareTo(candidateOrderingValue.get()) < 0) {
                        candidateOrderingValue = Optional.of(orderingValue);
                        candidate = maybeEvent;
                        candidateIndex = readerIndex;
                    }
                }
            }

            if (candidate.isPresent()) {
                currentPosition = currentPosition.withNextPosition(candidateIndex, candidate.get().position());
            }
            return candidate;
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

}
