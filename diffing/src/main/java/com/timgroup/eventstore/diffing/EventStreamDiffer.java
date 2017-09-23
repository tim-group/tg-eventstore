package com.timgroup.eventstore.diffing;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.stream.Stream;

public final class EventStreamDiffer {

    private final DiffListener listener;

    public EventStreamDiffer(DiffListener listener) {
        this.listener = listener;
    }

    public void diff(Stream<ResolvedEvent> streamA, Stream<ResolvedEvent> streamB) {
        PeekingIterator<DiffEvent> iteratorA = Iterators.peekingIterator(streamA.map(DiffEvent::from).iterator());
        PeekingIterator<DiffEvent> iteratorB = Iterators.peekingIterator(streamB.map(DiffEvent::from).iterator());

        while (iteratorA.hasNext() && iteratorB.hasNext()) {
            DiffEvent diffEventA = iteratorA.peek();
            DiffEvent diffEventB = iteratorB.peek();

            if (diffEventA.equals(diffEventB)) {
                listener.onMatchingEvents(iteratorA.next(), iteratorB.next());
            } else if (diffEventA.equalsExceptBody(diffEventB)) {
                listener.onSimilarEvents(iteratorA.next(), iteratorB.next());
            } else if (diffEventA.isEffectiveOnOrBefore(diffEventB)){
                listener.onUnmatchedEventInStreamA(iteratorA.next());
            } else {
                listener.onUnmatchedEventInStreamB(iteratorB.next());
            }
        }
        iteratorA.forEachRemaining(listener::onUnmatchedEventInStreamA);
        iteratorB.forEachRemaining(listener::onUnmatchedEventInStreamB);
    }
}
