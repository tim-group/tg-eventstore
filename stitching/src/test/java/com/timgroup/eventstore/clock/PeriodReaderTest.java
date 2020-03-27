package com.timgroup.eventstore.clock;

import com.google.common.collect.Lists;
import com.timgroup.clocks.testing.ManualClock;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class PeriodReaderTest {

    private final ManualClock clock = new ManualClock(Instant.parse("2018-06-06T00:00:00Z"), ZoneOffset.UTC);
    private final PeriodReader monthsReader = PeriodReader.monthsPassedEventStream(
                StreamId.streamId("periodic", "months"),
                LocalDate.of(2018, 4, 1),
                clock);

    @Test
    public void
    returns_periodic_events_from_start_date_to_now() {
        Stream<ResolvedEvent> eventStream = monthsReader.readAllForwards(monthsReader.emptyStorePosition());

        assertThat(extractEventTimestamps(eventStream),
                equalTo(Lists.newArrayList(
                        Instant.parse("2018-04-01T00:00:00Z"),
                        Instant.parse("2018-05-01T00:00:00Z"),
                        Instant.parse("2018-06-01T00:00:00Z"))));
    }

    @Test
    public void
    can_continue_reading_from_a_non_empty_position() {
        List<ResolvedEvent> initialEvents = monthsReader.readAllForwards(monthsReader.emptyStorePosition()).collect(Collectors.toList());
        ResolvedEvent lastEvent = initialEvents.get(initialEvents.size() - 1);
        List<ResolvedEvent> remainingEvents = monthsReader.readAllForwards(lastEvent.position()).collect(Collectors.toList());

        assertThat(remainingEvents, Matchers.empty());

        clock.bump(30, ChronoUnit.DAYS);

        Stream<ResolvedEvent> nextMonthEventStream = monthsReader.readAllForwards(lastEvent.position());
        assertThat(extractEventTimestamps(nextMonthEventStream),
                equalTo(Lists.newArrayList(Instant.parse("2018-07-01T00:00:00Z"))));
    }

    @Test
    public void
    days_passed_events() {
        PeriodReader daysReader = PeriodReader.daysPassedEventStream(
                StreamId.streamId("periodic", "days"),
                LocalDate.of(2018, 6, 4),
                clock);

        Stream<ResolvedEvent> eventStream = daysReader.readAllForwards(daysReader.emptyStorePosition());

        assertThat(extractEventTimestamps(eventStream),
                equalTo(Lists.newArrayList(
                        Instant.parse("2018-06-04T00:00:00Z"),
                        Instant.parse("2018-06-05T00:00:00Z"),
                        Instant.parse("2018-06-06T00:00:00Z"))));
    }

    private List<Instant> extractEventTimestamps(Stream<ResolvedEvent> events) {
        return events
                .map(ResolvedEvent::eventRecord)
                .map(EventRecord::timestamp)
                .collect(Collectors.toList());
    }

}
