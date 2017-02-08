package com.timgroup.eventstore.merging;

import com.google.common.collect.PeekingIterator;
import com.timgroup.eventstore.api.*;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Iterators.peekingIterator;
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

    @Override
    public Position emptyStorePosition() {
        Position[] positions = readers.stream().map(EventReader::emptyStorePosition).collect(toList()).toArray(new Position[0]);
        return new MergedEventReaderPosition(-1L, positions);
    }



    private static final class MergingSpliterator<T extends Comparable<T>> implements Spliterator<ResolvedEvent> {

        private final MergingStrategy<T> mergingStrategy;
        private final List<IdentifiedPeekingResolvedEventIterator> underlying;

        private MergedEventReaderPosition currentPosition;

        private MergingSpliterator(MergingStrategy<T> mergingStrategy, MergedEventReaderPosition startPosition, List<Stream<ResolvedEvent>> data) {
            this.mergingStrategy = mergingStrategy;
            this.currentPosition = startPosition;
            this.underlying = IdentifiedPeekingResolvedEventIterator.from(data);
        }

        @Override
        public boolean tryAdvance(Consumer<? super ResolvedEvent> consumer) {
            Optional<IdentifiedPeekingResolvedEventIterator> iteratorWhoseHeadIsNext = getIteratorWhoseHeadIsNext();

            iteratorWhoseHeadIsNext.ifPresent(iterator -> {
                ResolvedEvent nextInputEvent = iterator.next();
                currentPosition = currentPosition.withNextPosition(iteratorWhoseHeadIsNext.get().index, nextInputEvent.position());

                EventRecord record = nextInputEvent.eventRecord();
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
            });

            return iteratorWhoseHeadIsNext.isPresent();
        }

        private Optional<IdentifiedPeekingResolvedEventIterator> getIteratorWhoseHeadIsNext() {
            Optional<IdentifiedPeekingResolvedEventIterator> iteratorWhoseHeadIsNext = Optional.empty();
            Optional<T> iteratorWhoseHeadIsNextOrderingValue = Optional.empty();

            Iterator<IdentifiedPeekingResolvedEventIterator> streams = underlying.iterator();
            while (streams.hasNext()) {
                IdentifiedPeekingResolvedEventIterator candidate = streams.next();

                if (candidate.hasNext()) {
                    T candidateOrderingValue = mergingStrategy.toComparable(candidate.peek());
                    if (!iteratorWhoseHeadIsNext.isPresent() || candidateOrderingValue.compareTo(iteratorWhoseHeadIsNextOrderingValue.get()) < 0) {
                        iteratorWhoseHeadIsNext = Optional.of(candidate);
                        iteratorWhoseHeadIsNextOrderingValue = Optional.of(candidateOrderingValue);
                    }
                } else {
                    streams.remove();
                }
            }

            return iteratorWhoseHeadIsNext;
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

        private static final class IdentifiedPeekingResolvedEventIterator {
            private final int index;
            private final PeekingIterator<ResolvedEvent> delegate;

            private IdentifiedPeekingResolvedEventIterator(int index, PeekingIterator<ResolvedEvent> delegate) {
                this.index = index;
                this.delegate = delegate;
            }

            private static List<IdentifiedPeekingResolvedEventIterator> from(List<Stream<ResolvedEvent>> data) {
                return range(0, data.size())
                        .mapToObj(i -> new IdentifiedPeekingResolvedEventIterator(i, peekingIterator(data.get(i).iterator())))
                        .collect(toList());
            }

            public boolean hasNext() {
                return delegate.hasNext();
            }

            public ResolvedEvent peek() {
                return delegate.peek();
            }

            public ResolvedEvent next() {
                return delegate.next();
            }
        }
    }

}
