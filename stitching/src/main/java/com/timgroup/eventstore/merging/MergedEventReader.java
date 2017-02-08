package com.timgroup.eventstore.merging;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.timgroup.eventstore.api.*;

import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.collect.ImmutableList.copyOf;
import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static java.lang.Long.MAX_VALUE;
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

    private static final class MergingSpliterator<T extends Comparable<T>> implements Spliterator<ResolvedEvent> {

        private final MergingStrategy<T> mergingStrategy;
        private final List<PeekingIterator<ResolvedEvent>> underlying;

        private MergedEventReaderPosition currentPosition;

        private MergingSpliterator(MergingStrategy<T> mergingStrategy, MergedEventReaderPosition startPosition, List<Stream<ResolvedEvent>> data) {
            this.mergingStrategy = mergingStrategy;
            this.currentPosition = startPosition;
            this.underlying = data.stream().map(Stream::iterator).map(Iterators::peekingIterator).collect(toList());
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

            for (int readerIndex = 0; readerIndex < underlying.size(); readerIndex++) {
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
                underlying.get(candidateIndex).next();
            }
            return candidate;
        }

        private Optional<ResolvedEvent> peekNextFor(int readerIndex) {
            return underlying.get(readerIndex).hasNext() ? Optional.of(underlying.get(readerIndex).peek()) : Optional.empty();
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
        Position[] positions = readers.stream().map(EventReader::emptyStorePosition).collect(toList()).toArray(new Position[0]);
        return new MergedEventReaderPosition(-1L, positions);
    }

}
