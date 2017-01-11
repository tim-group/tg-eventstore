package com.timgroup.eventstore.common;

import com.timgroup.eventstore.api.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.stream.Stream;

import static com.timgroup.eventstore.api.EventStreamReader.EmptyStreamEventNumber;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.timgroup.eventstore.common.IdempotentEventStreamWriter.IsCompatible;

public final class IdempotentEventStreamWriter implements EventStreamWriter {

    public static class IncompatibleNewEventException extends RuntimeException {
        public final ResolvedEvent currentEvent;
        public final NewEvent newEvent;

        public IncompatibleNewEventException(String message, ResolvedEvent currentEvent, NewEvent newEvent) {
            super("at position " + currentEvent.position() + ": " + message);
            this.newEvent = newEvent;
            this.currentEvent = currentEvent;
        }
    }

    public interface IsCompatible {
        void throwIfIncompatible(ResolvedEvent currentEvent, NewEvent newEvent) throws IncompatibleNewEventException;
    }

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
        Iterator<NewEvent> newEventsIt = events.iterator();
        long newExpectedVersion = expectedVersion;
        try (Stream<ResolvedEvent> resolvedEventStream = reader.readStreamForwards(streamId, expectedVersion)){
            Iterator<ResolvedEvent> currentEventsIt = resolvedEventStream.iterator();

            while (newEventsIt.hasNext() && currentEventsIt.hasNext()) {
                newExpectedVersion += 1;
                NewEvent newEvent = newEventsIt.next();
                ResolvedEvent currentEvent = currentEventsIt.next();
                isCompatible.throwIfIncompatible(currentEvent, newEvent);
            }
        } catch (NoSuchStreamException e) {
            // Ignore
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
    }

    public static EventStreamWriter idempotent(EventStreamWriter underlying, EventStreamReader reader) {
        return idempotent(underlying, reader, BASIC_COMPATIBILITY_CHECK);
    }

    public static EventStreamWriter idempotentWithMetadataCheck(EventStreamWriter underlying, EventStreamReader reader) {
        return idempotent(underlying, reader, (a, b) -> {
            BASIC_COMPATIBILITY_CHECK.throwIfIncompatible(a, b);
            METADATA_COMPATIBILITY_CHECK.throwIfIncompatible(a, b);
        });
    }

    public static EventStreamWriter idempotent(EventStreamWriter underlying, EventStreamReader reader, IsCompatible isCompatible) {
        return new IdempotentEventStreamWriter(underlying, reader, isCompatible);
    }

    @SuppressWarnings("WeakerAccess")
    public static final IsCompatible BASIC_COMPATIBILITY_CHECK = (a, b) -> {
        if (!a.eventRecord().eventType().equals(b.type())) {
            throw new IncompatibleNewEventException("Event types don't match -- old: " + a.eventRecord().eventType() + ", new: " + b.type(), a, b);
        }
        if (!Arrays.equals(a.eventRecord().data(), b.data())) {
            throw new IncompatibleNewEventException("Event bodies don't match -- old: " + new String(a.eventRecord().data(), UTF_8) + ", new: " + new String(b.data(), UTF_8), a, b);
        }
    };

    @SuppressWarnings("WeakerAccess")
    public static final IsCompatible METADATA_COMPATIBILITY_CHECK = (a, b) -> {
        if (!Arrays.equals(a.eventRecord().metadata(), b.metadata())) {
            throw new IncompatibleNewEventException("Event metadata doesn't match -- old: " + new String(a.eventRecord().metadata(), UTF_8) + ", new: " + new String(b.metadata(), UTF_8), a, b);
        }
    };
}
