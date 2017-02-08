package com.timgroup.eventstore.merging;

import com.google.common.collect.PeekingIterator;
import com.timgroup.eventstore.api.*;

import java.util.Arrays;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.collect.Iterators.peekingIterator;
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
        MergedEventReaderPosition mergedPosition = (MergedEventReaderPosition) positionExclusive;

        Stream<ResolvedEvent>[] streams = new Stream[readers.length];
        for (int i = 0; i < readers.length; i++) {
            streams[i] = readers[i].readAllForwards(mergedPosition.inputPositions[i]);//.takeWhile(re -> re.timestamp.isbefore(snapNow.minus(delay)));
        }

        return StreamSupport.stream(new MergingSpliterator<>(mergingStrategy, mergedPosition, streams), false)
                .onClose(() -> Arrays.stream(streams).forEach(Stream::close));
    }

    private static final class MergingSpliterator<T extends Comparable<T>> implements Spliterator<ResolvedEvent> {

        private final MergingStrategy<T> mergingStrategy;
        private final PeekingIterator<ResolvedEvent>[] underlying;

        private MergedEventReaderPosition currentPosition;

        private MergingSpliterator(MergingStrategy<T> mergingStrategy, MergedEventReaderPosition startPosition, Stream<ResolvedEvent>... streams) {
            this.mergingStrategy = mergingStrategy;
            this.underlying = new PeekingIterator[streams.length];
            for (int i = 0; i < streams.length; i++) {
                this.underlying[i] = peekingIterator(streams[i].iterator());
            }
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

            for (int readerIndex = 0; readerIndex < underlying.length; readerIndex++) {
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
                underlying[candidateIndex].next();
            }
            return candidate;
        }

        private Optional<ResolvedEvent> peekNextFor(int readerIndex) {
            return underlying[readerIndex].hasNext() ? Optional.of(underlying[readerIndex].peek()) : Optional.empty();
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
