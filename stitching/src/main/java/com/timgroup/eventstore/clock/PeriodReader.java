package com.timgroup.eventstore.clock;

import com.google.common.collect.AbstractIterator;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class PeriodReader implements EventReader {
    private final String eventType;
    private final StreamId streamId;
    private final Period period;
    private final LocalDate startDate;
    private final Clock clock;

    public PeriodReader(String eventType, StreamId streamId, Period period, LocalDate startDate, Clock clock) {
        this.eventType = eventType;
        this.streamId = streamId;
        this.period = period;
        this.startDate = startDate;
        this.clock = clock;
    }

    @Nonnull
    public static PeriodReader daysPassedEventStream(StreamId streamId, LocalDate startDate, Clock clock) {
        return new PeriodReader("DayPassed", streamId, Period.ofDays(1), startDate, clock);
    }

    @Nonnull
    public static PeriodReader monthsPassedEventStream(StreamId streamId, LocalDate startDate, Clock clock) {
        return new PeriodReader("MonthPassed", streamId, Period.ofMonths(1), startDate, clock);
    }

    @Nonnull
    @Override
    public PositionCodec storePositionCodec() {
        return PeriodicEventStorePosition.CODEC;
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public Stream<ResolvedEvent> readAllForwards(@Nonnull Position positionExclusive) {
        final Instant upToTime = clock.instant();
        PeriodicEventStorePosition periodicPosition = (PeriodicEventStorePosition) positionExclusive;
        int firstIndex = Math.toIntExact(periodicPosition.eventNumber) + 1;

        return takeWhile(IntStream
                .iterate(firstIndex, i -> i + 1)
                .mapToObj(index -> {
                    LocalDate eventDate = startDate.plus(period.multipliedBy(index));
                    return resolvedEvent(eventDate, (long) index);
                }), event -> !event.eventRecord().timestamp().isAfter(upToTime));
    }

    private ResolvedEvent resolvedEvent(LocalDate eventDate, long eventNumber) {
        return new ResolvedEvent(
                new PeriodicEventStorePosition(eventNumber),
                EventRecord.eventRecord(
                        eventDate.atStartOfDay().toInstant(ZoneOffset.UTC),
                        streamId,
                        eventNumber,
                        eventType,
                        new byte[0],
                        new byte[0]));
    }

    private static <T> Stream<T> takeWhile(Stream<T> stream, Predicate<T> condition) {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(takeWhile(stream.iterator(), condition), Spliterator.ORDERED),
                false);
    }

    private static <T> Iterator<T> takeWhile(Iterator<T> eventStream, Predicate<T> condition) {
        return new AbstractIterator<T>() {
            @Override
            protected T computeNext() {
                if (eventStream.hasNext()) {
                    T event = eventStream.next();
                    if (condition.test(event)) {
                        return event;
                    }
                }
                return endOfData();
            }
        };
    }

    @Nonnull
    @Override
    public Position emptyStorePosition() {
        return new PeriodicEventStorePosition(-1);
    }

    private static final class PeriodicEventStorePosition implements Position, Comparable<PeriodicEventStorePosition> {
        private static final PositionCodec CODEC = PositionCodec.ofComparable(PeriodicEventStorePosition.class,
                str -> new PeriodicEventStorePosition(Long.parseLong(str)),
                pos -> Long.toString(pos.eventNumber));

        private final long eventNumber;

        private PeriodicEventStorePosition(long eventNumber) {
            this.eventNumber = eventNumber;
        }

        @Override public int compareTo(PeriodicEventStorePosition o) {
            return Long.compare(eventNumber, o.eventNumber);
        }

        @Override public boolean equals(@Nullable Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PeriodicEventStorePosition that = (PeriodicEventStorePosition) o;
            return eventNumber == that.eventNumber;

        }
        @Override public int hashCode() {
            return (int) (eventNumber ^ (eventNumber >>> 32));
        }
        @Override public String toString() {
            return Long.toString(eventNumber);
        }
    }
}
