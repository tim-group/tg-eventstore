package com.timgroup.eventstore.memory;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.NoSuchStreamException;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.api.WrongExpectedVersionException;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JavaInMemoryEventStore implements EventStreamWriter, EventStreamReader, EventCategoryReader, EventReader {
    public static final PositionCodec CODEC = PositionCodec.ofComparable(InMemoryEventStorePosition.class,
            str -> new InMemoryEventStorePosition(Long.parseLong(str)),
            pos -> Long.toString(pos.eventNumber));
    private final Collection<ResolvedEvent> events;
    private final Clock clock;

    public JavaInMemoryEventStore(Supplier<Collection<ResolvedEvent>> storageSupplier, Clock clock) {
        this.clock = clock;
        this.events = storageSupplier.get();
    }

    public JavaInMemoryEventStore(Clock clock) {
        this(CopyOnWriteArrayList::new, clock);
    }

    @Override
    @Nonnull
    @CheckReturnValue
    public Stream<ResolvedEvent> readStreamForwards(StreamId streamId, long eventNumberExclusive) {
        ensureStreamExists(streamId);
        return internalReadStream(streamId, eventNumberExclusive);
    }

    @Override
    @Nonnull
    @CheckReturnValue
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        InMemoryEventStorePosition inMemoryPosition = (InMemoryEventStorePosition) positionExclusive;
        return events.stream().skip(inMemoryPosition.eventNumber);
    }

    @Override
    @Nonnull
    @CheckReturnValue
    public Stream<ResolvedEvent> readAllBackwards() {
        List<ResolvedEvent> reversed = new ArrayList<>(events);
        Collections.reverse(reversed);
        return reversed.stream();
    }

    @Override
    @Nonnull
    @CheckReturnValue
    public Stream<ResolvedEvent> readAllBackwards(Position positionExclusive) {
        InMemoryEventStorePosition inMemoryPosition = (InMemoryEventStorePosition) positionExclusive;
        List<ResolvedEvent> reversed = events.stream().limit(Math.max(0, inMemoryPosition.eventNumber - 1)).collect(Collectors.toList());
        Collections.reverse(reversed);
        return reversed.stream();
    }

    @Override
    public synchronized void write(StreamId streamId, Collection<NewEvent> events) {
        write(streamId, events, currentVersionOf(streamId));
    }

    @Override
    @Nonnull
    @CheckReturnValue
    public Position emptyStorePosition() {
        return new InMemoryEventStorePosition(0);
    }

    @Nonnull
    @Override
    public PositionCodec storePositionCodec() {
        return CODEC;
    }

    @Override
    public synchronized void write(StreamId streamId, Collection<NewEvent> events, long expectedVersion) {
        long currentVersion = currentVersionOf(streamId);

        if (currentVersion != expectedVersion) {
            throw new WrongExpectedVersionException(currentVersion, expectedVersion);
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
        ))).forEachOrdered(this.events::add);
    }

    @Override
    @Nonnull
    @CheckReturnValue
    public Stream<ResolvedEvent> readCategoryForwards(String category, Position position) {
        return readAllForwards(position).filter(evt -> evt.eventRecord().streamId().category().equals(category));
    }

    @Nonnull
    @Override
    public Stream<ResolvedEvent> readCategoriesForwards(List<String> categories, Position positionExclusive) {
        Set<String> cats = new HashSet<>(categories);
        return readAllForwards(positionExclusive).filter(e -> cats.contains(e.eventRecord().streamId().category()));
    }

    @Override
    @Nonnull
    @CheckReturnValue
    public Stream<ResolvedEvent> readCategoryBackwards(String category) {
        return readAllBackwards().filter(evt -> evt.eventRecord().streamId().category().equals(category));
    }

    @Override
    @Nonnull
    @CheckReturnValue
    public Stream<ResolvedEvent> readCategoryBackwards(String category, Position position) {
        return readAllBackwards(position).filter(evt -> evt.eventRecord().streamId().category().equals(category));
    }

    @Override
    @Nonnull
    @CheckReturnValue
    public Stream<ResolvedEvent> readStreamBackwards(StreamId streamId) {
        ensureStreamExists(streamId);
        return readAllBackwards().filter(evt -> evt.eventRecord().streamId().equals(streamId));
    }

    @Override
    @Nonnull
    @CheckReturnValue
    public Stream<ResolvedEvent> readStreamBackwards(StreamId streamId, long eventNumberExclusive) {
        ensureStreamExists(streamId);
        return readAllBackwards()
                .filter(evt -> evt.eventRecord().streamId().equals(streamId))
                .filter(evt -> evt.eventRecord().eventNumber() < eventNumberExclusive);
    }

    @Nonnull
    @Override
    public PositionCodec streamPositionCodec() {
        return CODEC;
    }

    @Override
    @Nonnull
    public Position emptyCategoryPosition(String category) {
        return emptyStorePosition();
    }

    @Nonnull
    @Override
    public PositionCodec categoryPositionCodec() {
        return CODEC;
    }

    private long currentVersionOf(StreamId streamId) {
        return internalReadStream(streamId, EmptyStreamEventNumber)
                .mapToLong(r -> r.eventRecord().eventNumber())
                .max()
                .orElse(EmptyStreamEventNumber);
    }

    private void ensureStreamExists(StreamId streamId) {
        if (!internalReadStream(streamId, Long.MIN_VALUE)
                .findAny()
                .isPresent()) {
            throw new NoSuchStreamException(streamId);
        }
    }

    private Stream<ResolvedEvent> internalReadStream(StreamId streamId, long eventNumberExclusive) {
        return events.stream()
                .filter(event -> event.eventRecord().streamId().equals(streamId))
                .filter(event -> event.eventRecord().eventNumber() > eventNumberExclusive);
    }

    @Override
    public String toString() {
        return "JavaInMemoryEventStore{" +
                "events.size=" + events.size() +
                ", clock=" + clock +
                '}';
    }

    static final class InMemoryEventStorePosition implements Position, Comparable<InMemoryEventStorePosition> {

        private final long eventNumber;

        private InMemoryEventStorePosition(long eventNumber) {
            this.eventNumber = eventNumber;
        }

        @Override
        public int compareTo(InMemoryEventStorePosition o) {
            return Long.compare(eventNumber, o.eventNumber);
        }

        @Override
        public boolean equals(@Nullable Object o) {
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
            return Long.toString(eventNumber);
        }
    }
}
