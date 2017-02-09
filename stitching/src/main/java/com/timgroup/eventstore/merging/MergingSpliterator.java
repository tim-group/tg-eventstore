package com.timgroup.eventstore.merging;

import com.google.common.collect.PeekingIterator;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;

import static com.google.common.collect.Iterators.peekingIterator;
import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static java.lang.Long.MAX_VALUE;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

final class MergingSpliterator<T extends Comparable<T>> implements Spliterator<ResolvedEvent> {

    private final MergingStrategy<T> mergingStrategy;
    private final List<IdentifiedPeekingResolvedEventIterator> underlying;

    private MergedEventReaderPosition currentPosition;

    MergingSpliterator(MergingStrategy<T> mergingStrategy, MergedEventReaderPosition currentPosition, List<Iterator<ResolvedEvent>> data) {
        this.mergingStrategy = mergingStrategy;
        this.currentPosition = currentPosition;
        this.underlying = IdentifiedPeekingResolvedEventIterator.from(data);
    }

    @Override
    public boolean tryAdvance(Consumer<? super ResolvedEvent> consumer) {
        Optional<IdentifiedPeekingResolvedEventIterator> iteratorWhoseHeadIsNext = getIteratorWhoseHeadIsNext();

        iteratorWhoseHeadIsNext.ifPresent(iterator -> {
            ResolvedEvent nextInputEvent = iterator.next();
            currentPosition = currentPosition.withNextPosition(iterator.index, nextInputEvent.position());

            EventRecord record = nextInputEvent.eventRecord();
            consumer.accept(new ResolvedEvent(
                    currentPosition,
                    eventRecord(
                            record.timestamp(),
                            mergingStrategy.mergedStreamId(),
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

        private static List<IdentifiedPeekingResolvedEventIterator> from(List<Iterator<ResolvedEvent>> data) {
            return range(0, data.size())
                    .mapToObj(i -> new IdentifiedPeekingResolvedEventIterator(i, peekingIterator(data.get(i))))
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
