package com.timgroup.eventstore.memory;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.api.WrongExpectedVersion;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class JavaInMemoryEventStore implements EventStreamWriter, EventStreamReader, EventCategoryReader, EventReader {
    private final Collection<ResolvedEvent> events = new ArrayList<>();
    private final Clock clock;

    public JavaInMemoryEventStore(Clock clock) {
        this.clock = clock;
    }

    @Override
    public Stream<ResolvedEvent> readStreamForwards(StreamId streamId, long eventNumberExclusive) {
        return events.stream()
                .filter(event -> event.eventRecord().streamId().equals(streamId))
                .filter(event -> event.eventRecord().eventNumber() > eventNumberExclusive);
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards() {
        return events.stream();
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        InMemoryEventStorePosition inMemoryPosition = (InMemoryEventStorePosition) positionExclusive;
        return readAllForwards().skip(inMemoryPosition.eventNumber);
    }

    @Override
    public void write(StreamId streamId, Collection<NewEvent> events) {
        write(streamId, events, currentVersionOf(streamId));
    }

    private long currentVersionOf(StreamId streamId) {
        return readStreamForwards(streamId, EmptyStreamEventNumber).reduce((a, b) -> b)
                .map(ResolvedEvent::eventRecord)
                .map(EventRecord::eventNumber)
                .orElse(-1L);
    }

    @Override
    public void write(StreamId streamId, Collection<NewEvent> events, long expectedVersion) {
        long currentVersion = currentVersionOf(streamId);

        if (currentVersion != expectedVersion) {
            throw new WrongExpectedVersion();
        }

        AtomicLong globalPosition = new AtomicLong(this.events.size());
        AtomicLong eventNumber = new AtomicLong(currentVersion);

        events.stream().map(newEvent -> new ResolvedEvent(new InMemoryEventStorePosition(globalPosition.incrementAndGet()), EventRecord.eventRecord(
                clock.instant(),
                streamId,
                eventNumber.incrementAndGet(),
                newEvent.type(),
                newEvent.data(),
                newEvent.metadata()
        ))).forEach(this.events::add);
    }

    @Override
    public Stream<ResolvedEvent> readCategoryForwards(String category) {
        return readAllForwards().filter(evt -> evt.eventRecord().streamId().category().equals(category));
    }

    @Override
    public Stream<ResolvedEvent> readCategoryForwards(String category, Position position) {
        return readAllForwards(position).filter(evt -> evt.eventRecord().streamId().category().equals(category));
    }

    private static class InMemoryEventStorePosition implements Position {
        private final long eventNumber;

        private InMemoryEventStorePosition(long eventNumber) {
            this.eventNumber = eventNumber;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            InMemoryEventStorePosition that = (InMemoryEventStorePosition) o;

            return eventNumber == that.eventNumber;

        }

        @Override
        public int hashCode() {
            return (int) (eventNumber ^ (eventNumber >>> 32));
        }

        @Override
        public String toString() {
            return "InMemoryEventStorePosition{" +
                    "eventNumber=" + eventNumber +
                    '}';
        }
    }
}
