package com.timgroup.eventstore.stitching;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static java.util.stream.Collectors.toList;

public final class EventStoreStitchingIterator implements Iterator<EventInIdentifiedStream> {
    private final Clock clock;
    private final Duration delay;
    private final List<PeekingIterator<EventInIdentifiedStream>> underlying;
    private final Comparator<EventInIdentifiedStream> order;

    private Long cutOffTime;
    private PeekingIterator<EventInIdentifiedStream> iteratorWhoseHeadIsNext;

    public EventStoreStitchingIterator(Clock clock, Duration delay, List<Iterator<EventInIdentifiedStream>> iterators) {
        this(clock, delay, iterators, (a, b) -> a.event.effectiveTimestamp().compareTo(b.event.effectiveTimestamp()));
    }

    public EventStoreStitchingIterator(Clock clock, Duration delay, List<Iterator<EventInIdentifiedStream>> iterators, Comparator<EventInIdentifiedStream> order) {
        this.clock = clock;
        this.delay = delay;
        this.underlying = iterators.stream().map(Iterators::peekingIterator).collect(toList());
        this.order = order;
    }

    @Override
    public boolean hasNext() {
        return getIteratorWhoseHeadIsNext() != null;
    }

    @Override
    public EventInIdentifiedStream next() {
        Iterator<EventInIdentifiedStream> candidate = getIteratorWhoseHeadIsNext();
        if (candidate == null) {
            throw new NoSuchElementException();
        }
        iteratorWhoseHeadIsNext = null;
        return candidate.next();
    }

    private Iterator<EventInIdentifiedStream> getIteratorWhoseHeadIsNext() {
        if (iteratorWhoseHeadIsNext != null) {
            return iteratorWhoseHeadIsNext;
        }
        Iterator<PeekingIterator<EventInIdentifiedStream>> streams = underlying.iterator();
        while (streams.hasNext()) {
            PeekingIterator<EventInIdentifiedStream> eventStream = streams.next();

            Instant potentialCutoffTime = clock.instant();
            if (eventStream.hasNext()) {
                if (cutOffTime != null && eventStream.peek().event.effectiveTimestamp().isAfter(cutOffTime)) {
                    streams.remove();
                } else if (iteratorWhoseHeadIsNext == null || order.compare(eventStream.peek(), iteratorWhoseHeadIsNext.peek()) < 0) {
                    iteratorWhoseHeadIsNext = eventStream;
                }
            } else {
                streams.remove();
                if (this.cutOffTime == null) {
                    this.cutOffTime = potentialCutoffTime.minus(delay).toEpochMilli();
                }
            }
        }

        if (iteratorWhoseHeadIsNext != null) {
            long cutoff = this.cutOffTime == null ? clock.instant().minus(delay).toEpochMilli() : this.cutOffTime;
            if (iteratorWhoseHeadIsNext.peek().event.effectiveTimestamp().isAfter(cutoff)) {
                underlying.clear();
                return null;
            }
        }
        return iteratorWhoseHeadIsNext;
    }
}
