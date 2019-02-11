package com.timgroup.eventstore.merging;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.Iterators.peekingIterator;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

final class MergingIterator<T extends Comparable<T>> extends AbstractIterator<ResolvedEvent> {

    private final MergingStrategy<T> mergingStrategy;
    private final List<IdentifiedPeekingResolvedEventIterator> underlying;

    private MergedEventReaderPosition currentPosition;

    MergingIterator(MergingStrategy<T> mergingStrategy, MergedEventReaderPosition currentPosition, List<Iterator<ResolvedEvent>> data) {
        this.mergingStrategy = mergingStrategy;
        this.currentPosition = currentPosition;
        this.underlying = IdentifiedPeekingResolvedEventIterator.from(data);
    }

    @Override
    protected ResolvedEvent computeNext() {
        IdentifiedPeekingResolvedEventIterator iterator = getIteratorWhoseHeadIsNext();

        if (iterator == null) {
            return endOfData();
        }

        ResolvedEvent nextInputEvent = iterator.next();
        currentPosition = currentPosition.withNextPosition(iterator.index, nextInputEvent.position());

        return nextInputEvent.eventRecord().toResolvedEvent(currentPosition);
    }

    private IdentifiedPeekingResolvedEventIterator getIteratorWhoseHeadIsNext() {
        IdentifiedPeekingResolvedEventIterator iteratorWhoseHeadIsNext = null;
        T iteratorWhoseHeadIsNextOrderingValue = null;

        Iterator<IdentifiedPeekingResolvedEventIterator> streams = underlying.iterator();
        while (streams.hasNext()) {
            IdentifiedPeekingResolvedEventIterator candidate = streams.next();

            if (candidate.hasNext()) {
                T candidateOrderingValue = mergingStrategy.toComparable(candidate.peek());
                if (iteratorWhoseHeadIsNext == null || candidateOrderingValue.compareTo(iteratorWhoseHeadIsNextOrderingValue) < 0) {
                    iteratorWhoseHeadIsNext = candidate;
                    iteratorWhoseHeadIsNextOrderingValue = candidateOrderingValue;
                }
            } else {
                streams.remove();
            }
        }

        return iteratorWhoseHeadIsNext;
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
