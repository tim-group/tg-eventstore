package com.timgroup.eventstore.merging;

import com.google.common.collect.PeekingIterator;
import com.timgroup.eventstore.api.*;

import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.collect.Iterators.peekingIterator;
import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static java.lang.Long.MAX_VALUE;
import static java.util.Arrays.fill;
import static java.util.Collections.emptyIterator;
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
        private final PeekingIterator<ResolvedEvent>[] currentBatch;


        private MergedEventReaderPosition currentPosition;

        private MergingSpliterator(MergingStrategy<T> mergingStrategy, MergedEventReaderPosition startPosition, EventReader... readers) {
            this.mergingStrategy = mergingStrategy;
            this.readers = readers;
            this.currentBatch = new PeekingIterator[readers.length];
            fill(this.currentBatch, peekingIterator(emptyIterator()));

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
                                StreamId.streamId("input", "all"),  //TODO: make this configurable
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
                Optional<ResolvedEvent> maybeEvent = peekNextFor(readerIndex);
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
                currentBatch[candidateIndex].next();
            }
            return candidate;
        }

        private Optional<ResolvedEvent> peekNextFor(int readerIndex) {
            Position pos = currentPosition.inputPositions[readerIndex];

            if (currentBatch[readerIndex].hasNext()) {
                return Optional.of(currentBatch[readerIndex].peek());
            }

            currentBatch[readerIndex] = peekingIterator(readers[readerIndex].readAllForwards(pos).iterator());
            return currentBatch[readerIndex].hasNext() ? Optional.of(currentBatch[readerIndex].peek()) : Optional.empty();
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
