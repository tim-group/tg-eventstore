package com.timgroup.eventstore.stitching;

import com.timgroup.eventstore.api.*;
import com.timgroup.eventstore.memory.InMemoryEventSource;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

public final class BackfillStitchingEventSourceTest  {

    private java.time.Clock now = java.time.Clock.systemUTC();
    private final EventSource backfill = new InMemoryEventSource(new JavaInMemoryEventStore(now));
    private final EventSource live = new InMemoryEventSource(new JavaInMemoryEventStore(now));

    private BackfillStitchingEventSource eventSource;

    @Before public void setup() {
        backfill.writeStream().write(
                StreamId.streamId("all", "all"),
                Arrays.asList(
                        event("B1"),
                        event("B2"),
                        event("B3")));

        live.writeStream().write(
                StreamId.streamId("all", "all"),
                Arrays.asList(
                        event("old-1"),
                        event("old-2"),
                        event("old-3"),
                        event("old-4"),
                        event("L1"),
                        event("L2")));

        Position stitchPosition = live.positionCodec().deserializePosition("4");
        eventSource = new BackfillStitchingEventSource(backfill, live, stitchPosition);
    }

    @Test public void
    it_reads_all_events_from_backfill_and_those_required_from_live_when_querying_entire_eventstream() {
        List<NewEvent> readEvents = eventSource.readAll().readAllForwards()
                .map(ResolvedEvent::eventRecord)
                .map(p -> newEvent(p.eventType(), p.data(), p.metadata()))
                .collect(toList());

        assertThat(readEvents, contains(
                event("B1"),
                event("B2"),
                event("B3"),
                event("L1"),
                event("L2")
        ));
    }

    @Test public void
    it_returns_only_events_from_live_if_cutoff_is_before_fromVersion() {
        Position startPosition = eventSource.readAll().readAllForwards().limit(4).collect(toList()).get(3).position();

        List<NewEvent> readEvents = eventSource.readAll().readAllForwards(startPosition)
                .map(ResolvedEvent::eventRecord)
                .map(p -> newEvent(p.eventType(), p.data(), p.metadata()))
                .collect(toList());

        assertThat(readEvents, contains(
                event("L2")
        ));
    }

    private NewEvent event(String eventType) {
        return newEvent(
                eventType,
                eventType.getBytes(UTF_8),
                new StringBuilder(eventType).reverse().toString().getBytes(UTF_8)
        );
    }

}
