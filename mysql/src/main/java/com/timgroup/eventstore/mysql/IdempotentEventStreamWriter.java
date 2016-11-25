package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.*;

import java.util.*;
import java.util.stream.Stream;

public class IdempotentEventStreamWriter implements EventStreamWriter {
    private final EventStreamWriter underlying;
    private final EventStreamReader reader;
    private final CompatibilityPredicate isCompatible;

    private IdempotentEventStreamWriter(EventStreamWriter underlying, EventStreamReader reader, CompatibilityPredicate isCompatible) {
        this.underlying = underlying;
        this.reader = reader;
        this.isCompatible = isCompatible;
    }

    @Override
    public void write(StreamId streamId, Collection<NewEvent> events) {
        // TODO, use Stream<ResolvedEvent> currentEvents = reader.readStreamForwards(streamId);
        throw new RuntimeException("Not yet implemented");
    }

    @Override
    public void write(StreamId streamId, Collection<NewEvent> events, long expectedVersion) {
        Stream<ResolvedEvent> currentEvents = reader.readStreamForwards(streamId, expectedVersion);
        Iterator<ResolvedEvent> currentEventsIt = currentEvents.iterator();
        Iterator<NewEvent> newEventsIt = events.iterator();

        try {
            long newExpectedVersion = expectedVersion;
            while (newEventsIt.hasNext() && currentEventsIt.hasNext()) {
                newExpectedVersion += 1;
                NewEvent newEvent = newEventsIt.next();
                ResolvedEvent currentEvent = currentEventsIt.next();
                isCompatible.test(currentEvent, newEvent);
            }

            if (newEventsIt.hasNext()) {
                final Collection<NewEvent> eventsToSave = new LinkedList<>();
                newEventsIt.forEachRemaining(eventsToSave::add);
                underlying.write(streamId, eventsToSave, newExpectedVersion);
            }
        } finally {
            currentEvents.close();
        }
    }

    public static EventStreamWriter idempotent(EventStreamWriter underlying, EventStreamReader reader) {
        return idempotent(underlying, reader, (a, b) -> {
            if (!a.eventRecord().eventType().equals(b.type())) {
                throw new IdempotentWriteFailure("Event types don't match -- old: " + a.eventRecord().eventType() + ", new: " + b.type());
            }
            if (!Arrays.equals(a.eventRecord().data(), b.data())) {
                throw new IdempotentWriteFailure("Event bodies don't match -- old: " + new String(a.eventRecord().data()) + ", new: " + new String(b.data()));
            }
            return;
        });
    }

    public static EventStreamWriter idempotent(EventStreamWriter underlying, EventStreamReader reader, CompatibilityPredicate isCompatible) {
        return new IdempotentEventStreamWriter(underlying, reader, isCompatible);
    }

    public interface CompatibilityPredicate {
        void test(ResolvedEvent currentEvent, NewEvent newEvent) throws IdempotentWriteFailure;
    }
}
