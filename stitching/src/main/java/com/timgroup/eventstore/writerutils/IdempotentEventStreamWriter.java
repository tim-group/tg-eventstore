package com.timgroup.eventstore.writerutils;

import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.NoSuchStreamException;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.api.WrongExpectedVersionException;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.function.BiPredicate;
import java.util.stream.Stream;

import static com.timgroup.eventstore.api.EventStreamReader.EmptyStreamEventNumber;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

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

    @FunctionalInterface
    public interface IsCompatible {
        void throwIfIncompatible(ResolvedEvent currentEvent, NewEvent newEvent) throws IncompatibleNewEventException;

        default IsCompatible and(IsCompatible other) {
            return (currentEvent, newEvent) -> {
                throwIfIncompatible(currentEvent, newEvent);
                other.throwIfIncompatible(currentEvent, newEvent);
            };
        }

        static IsCompatible byComparingEventType(BiPredicate<String, String> predicate) {
            return (currentEvent, newEvent) -> {
                if (!predicate.test(currentEvent.eventRecord().eventType(), newEvent.type())) {
                    throw new IncompatibleNewEventException(
                            String.format(
                                    "Event types don't match -- old: %s, new: %s (bodies old: %s, new %s)",
                                    currentEvent.eventRecord().eventType(),
                                    newEvent.type(),
                                    new String(currentEvent.eventRecord().data(), UTF_8),
                                    new String(newEvent.data(), UTF_8)
                            ),
                            currentEvent,
                            newEvent
                    );
                }
            };
        }

        static IsCompatible byComparingData(BiPredicate<byte[], byte[]> predicate) {
            return (currentEvent, newEvent) -> {
                if (!predicate.test(currentEvent.eventRecord().data(), newEvent.data())) {
                    throw new IncompatibleNewEventException(
                            String.format(
                                    "Event bodies don't match -- old: %s, new: %s (type %s)",
                                    new String(currentEvent.eventRecord().data(), UTF_8),
                                    new String(newEvent.data(), UTF_8),
                                    currentEvent.eventRecord().eventType()
                            ),
                            currentEvent,
                            newEvent
                    );
                }
            };
        }

        static IsCompatible byComparingMetadata(BiPredicate<byte[], byte[]> predicate) {
            return (currentEvent, newEvent) -> {
                if (!predicate.test(currentEvent.eventRecord().metadata(), newEvent.metadata())) {
                    throw new IncompatibleNewEventException(
                            String.format(
                                    "Event metadata doesn't match -- old: %s, new: %s",
                                    new String(currentEvent.eventRecord().metadata(), UTF_8),
                                    new String(newEvent.metadata(), UTF_8)
                            ),
                            currentEvent,
                            newEvent);
                }
            };
        }
    }

    private final EventStreamWriter underlying;
    private final EventStreamReader reader;
    private final IsCompatible isCompatible;

    private IdempotentEventStreamWriter(EventStreamWriter underlying, EventStreamReader reader, IsCompatible isCompatible) {
        this.underlying = requireNonNull(underlying);
        this.reader = requireNonNull(reader);
        this.isCompatible = requireNonNull(isCompatible);
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

    @Override
    public String toString() {
        return "IdempotentEventStreamWriter{" +
                "underlying=" + underlying +
                ", reader=" + reader +
                ", isCompatible=" + isCompatible +
                '}';
    }

    public static EventStreamWriter idempotent(EventStreamWriter underlying, EventStreamReader reader) {
        return idempotent(underlying, reader, BASIC_COMPATIBILITY_CHECK);
    }

    public static EventStreamWriter idempotentWithMetadataCheck(EventStreamWriter underlying, EventStreamReader reader) {
        return idempotent(underlying, reader, BASIC_COMPATIBILITY_CHECK.and(METADATA_COMPATIBILITY_CHECK));
    }

    public static EventStreamWriter idempotent(EventStreamWriter underlying, EventStreamReader reader, IsCompatible isCompatible) {
        return new IdempotentEventStreamWriter(underlying, reader, isCompatible);
    }

    public static final IsCompatible BASIC_COMPATIBILITY_CHECK =
            IsCompatible.byComparingEventType(Object::equals)
            .and(IsCompatible.byComparingData(Arrays::equals));

    public static final IsCompatible METADATA_COMPATIBILITY_CHECK = IsCompatible.byComparingMetadata(Arrays::equals);
}
