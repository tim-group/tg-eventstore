package com.timgroup.eventstore.common;

import com.timgroup.eventstore.api.*;

import java.util.*;
import java.util.stream.Stream;

import static com.timgroup.eventstore.api.EventStreamReader.EmptyStreamEventNumber;

public class IdempotentEventStreamWriter implements EventStreamWriter {
    private final EventStreamWriter underlying;
    private final EventStreamReader reader;
    private final IsCompatible isCompatible;

    private IdempotentEventStreamWriter(EventStreamWriter underlying, EventStreamReader reader, IsCompatible isCompatible) {
        this.underlying = underlying;
        this.reader = reader;
        this.isCompatible = isCompatible;
    }

    @Override
    public void write(StreamId streamId, Collection<NewEvent> events) throws IncompatibleNewEventException {
        write(streamId, events, EmptyStreamEventNumber);
    }

    @Override
    public void write(StreamId streamId, Collection<NewEvent> events, long expectedVersion) throws IncompatibleNewEventException {
        Stream<ResolvedEvent> currentEvents = reader.readStreamForwards(streamId, expectedVersion);
        Iterator<ResolvedEvent> currentEventsIt = currentEvents.iterator();
        Iterator<NewEvent> newEventsIt = events.iterator();

        try {
            long newExpectedVersion = expectedVersion;
            while (newEventsIt.hasNext() && currentEventsIt.hasNext()) {
                newExpectedVersion += 1;
                NewEvent newEvent = newEventsIt.next();
                ResolvedEvent currentEvent = currentEventsIt.next();
                isCompatible.throwIfIncompatible(currentEvent, newEvent);
            }

            if (newEventsIt.hasNext()) {
                final Collection<NewEvent> newEventsToSave = new LinkedList<>();
                newEventsIt.forEachRemaining(newEventsToSave::add);
                try {
                    underlying.write(streamId, newEventsToSave, newExpectedVersion);
                } catch (WrongExpectedVersionException e) {
                    write(streamId, newEventsToSave, newExpectedVersion);
                }
            }
        } finally {
            currentEvents.close();
        }
    }

    public static EventStreamWriter idempotent(EventStreamWriter underlying, EventStreamReader reader) {
        return idempotent(underlying, reader, (a, b) -> {
            if (!a.eventRecord().eventType().equals(b.type())) {
                throw new IncompatibleNewEventException("Event types don't match -- old: " + a.eventRecord().eventType() + ", new: " + b.type(), a, b);
            }
            if (!Arrays.equals(a.eventRecord().data(), b.data())) {
                throw new IncompatibleNewEventException("Event bodies don't match -- old: " + new String(a.eventRecord().data()) + ", new: " + new String(b.data()), a, b);
            }
            return;
        });
    }

    public static EventStreamWriter idempotent(EventStreamWriter underlying, EventStreamReader reader, IsCompatible isCompatible) {
        return new IdempotentEventStreamWriter(underlying, reader, isCompatible);
    }

    public interface IsCompatible {
        void throwIfIncompatible(ResolvedEvent currentEvent, NewEvent newEvent) throws IncompatibleNewEventException;
    }

    public static class IncompatibleNewEventException extends RuntimeException {
        final public ResolvedEvent currentEvent;
        final public NewEvent newEvent;

        public IncompatibleNewEventException(String message, ResolvedEvent currentEvent, NewEvent newEvent) {
            super(message);
            this.newEvent = newEvent;
            this.currentEvent = currentEvent;
        }
    }
}
