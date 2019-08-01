package com.timgroup.eventstore.readerutils;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.Stream.concat;
import static java.util.stream.StreamSupport.stream;

public final class ReorderingEventReader<T extends Comparable<T>> implements EventReader {
    private final EventReader underlying;
    private final T cutoffSortKey;
    private final Function<ResolvedEvent, T> sortKeyExtractor;

    public ReorderingEventReader(EventReader underlying, T cutoffSortKey, Function<ResolvedEvent, T> sortKeyExtractor) {
        this.underlying = requireNonNull(underlying);
        this.cutoffSortKey = requireNonNull(cutoffSortKey);
        this.sortKeyExtractor = requireNonNull(sortKeyExtractor);
    }

    @Nonnull
    @Override
    public Position emptyStorePosition() {
        return underlying.emptyStorePosition();
    }

    @Nonnull
    @Override
    public PositionCodec storePositionCodec() {
        return underlying.storePositionCodec();
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        try(Stream<ResolvedEvent> allForwards = underlying.readAllForwards(positionExclusive)) {
            PeekingIterator<ResolvedEvent> allForwardsIterator = Iterators.peekingIterator(allForwards.iterator());

            if (allForwardsIterator.hasNext() && sortKeyExtractor.apply(allForwardsIterator.peek()).compareTo(cutoffSortKey) < 0) {
                allForwards.close();
                return bufferedAndSortedReadAllForwards(positionExclusive);
            }

            return stream(spliteratorUnknownSize(allForwardsIterator, ORDERED), false).onClose(allForwards::close);
        }
    }

    @Override
    public String toString() {
        return "ReorderingEventReader{" +
                "underlying=" + underlying +
                ", cutoffSortKey=" + cutoffSortKey +
                ", sortKeyExtractor=" + sortKeyExtractor +
                '}';
    }

    private Stream<ResolvedEvent> bufferedAndSortedReadAllForwards(Position positionExclusive) {
        try(Stream<ResolvedEvent> allForwards = underlying.readAllForwards()) {

            Iterator<ResolvedEvent> remainder = allForwards.iterator();
            PeekingIterator<EventWithSortKey<T>> sortCandidates = Iterators.peekingIterator(
                    Iterators.transform(remainder, re -> new EventWithSortKey<>(re, sortKeyExtractor.apply(re)))
            );

            final LinkedList<EventWithSortKey<T>> buffer = new LinkedList<>();

            while (sortCandidates.hasNext() && sortCandidates.peek().sortKey.compareTo(cutoffSortKey) < 0) {
                buffer.add(sortCandidates.next());
            }

            if (!sortCandidates.hasNext()) {
                return Stream.empty();
            }

            buffer.sort(Comparator.naturalOrder());

            if (!positionExclusive.equals(underlying.emptyStorePosition())) {
                Iterator<EventWithSortKey<T>> bufferIterator = buffer.iterator();
                while (!bufferIterator.next().event.position().equals(positionExclusive)) {
                    bufferIterator.remove();
                }
                bufferIterator.remove();
            }

            Stream<EventWithSortKey<T>> reorderedEvents = buffer.stream().onClose(buffer::clear);
            Stream<EventWithSortKey<T>> eventInTheGap = Stream.of(sortCandidates.peek());
            Stream<ResolvedEvent> remainingEvents = stream(spliteratorUnknownSize(remainder, ORDERED), false);

            return concat(concat(reorderedEvents, eventInTheGap).map(EventWithSortKey::toResolvedEvent), remainingEvents).onClose(allForwards::close);
        }
    }

    private static final class EventWithSortKey<T extends Comparable<T>> implements Comparable<EventWithSortKey<T>> {
        private final ResolvedEvent event;
        private final T sortKey;

        public EventWithSortKey(ResolvedEvent event, T sortKey) {
            this.event = event;
            this.sortKey = sortKey;
        }

        @Override
        public int compareTo(EventWithSortKey<T> o) {
            return sortKey.compareTo(o.sortKey);
        }

        @Override
        public boolean equals(@Nullable Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EventWithSortKey that = (EventWithSortKey) o;
            return Objects.equals(event, that.event) &&
                    Objects.equals(sortKey, that.sortKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(event, sortKey);
        }

        public ResolvedEvent toResolvedEvent() {
            return event;
        }
    }
}
