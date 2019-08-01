package com.timgroup.eventstore.stitching;

import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.memory.InMemoryEventSource;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

public final class BackfillStitchingEventSourceTest  {

    private java.time.Clock now = java.time.Clock.systemUTC();
    private final EventSource backfill = new InMemoryEventSource(new JavaInMemoryEventStore(now));
    private final EventSource live = new InMemoryEventSource(new JavaInMemoryEventStore(now));

    private BackfillStitchingEventSource eventSource;

    @Before public void setup() {
        EventStreamWriter backfillWriter = backfill.writeStream();
        backfillWriter.write(StreamId.streamId("category_1", "stream1"), Collections.singleton(event("B1")));
        backfillWriter.write(StreamId.streamId("category_2", "streamx"), Collections.singleton(event("B2")));
        backfillWriter.write(StreamId.streamId("category_1", "stream2"), Collections.singleton(event("B3")));

        EventStreamWriter liveWriter = live.writeStream();
        liveWriter.write(StreamId.streamId("category_1", "stream1"), Collections.singleton(event("old-1")));
        liveWriter.write(StreamId.streamId("category_2", "streamx"), Collections.singleton(event("old-2")));
        liveWriter.write(StreamId.streamId("category_1", "stream2"), Collections.singleton(event("old-3")));
        liveWriter.write(StreamId.streamId("category_2", "streamx"), Collections.singleton(event("old-4")));
        liveWriter.write(StreamId.streamId("category_1", "stream1"), Collections.singleton(event("L1")));
        liveWriter.write(StreamId.streamId("category_2", "streamx"), Collections.singleton(event("L2")));
        liveWriter.write(StreamId.streamId("category_1", "stream2"), Collections.singleton(event("L3")));


        Position stitchPosition = live.readAll().storePositionCodec().deserializePosition("4");
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
                event("L2"),
                event("L3")
        ));
    }

    @Test public void
    generates_stitched_positions() {
        List<String> readEvents = eventSource.readAll().readAllForwards()
                .map(ResolvedEvent::position)
                .map(p -> eventSource.readAll().storePositionCodec().serializePosition(p))
                .collect(toList());

        assertThat(readEvents, contains(
                "1~~~4",
                "2~~~4",
                "3~~~4",
                "3~~~5",
                "3~~~6",
                "3~~~7"
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
                event("L2"),
                event("L3")
        ));
    }

    @Test
    public void
    can_read_events_by_category() {
        List<NewEvent> readEvents = eventSource.readCategory().readCategoryForwards("category_1")
                .map(ResolvedEvent::eventRecord)
                .map(p -> newEvent(p.eventType(), p.data(), p.metadata()))
                .collect(toList());

        assertThat(readEvents, contains(
                event("B1"),
                event("B3"),
                event("L1"),
                event("L3")
        ));
    }

    @Test
    public void
    can_continue_reading_from_position_of_category_in_backfill() {
        Position startPosition = eventSource.readAll().readAllForwards().limit(1).collect(toList()).get(0).position();

        List<NewEvent> readEvents = eventSource.readCategory().readCategoryForwards("category_1", startPosition)
                .map(ResolvedEvent::eventRecord)
                .map(p -> newEvent(p.eventType(), p.data(), p.metadata()))
                .collect(toList());

        assertThat(readEvents, contains(
                event("B3"),
                event("L1"),
                event("L3")
        ));
    }

    @Test
    public void
    can_continue_reading_from_position_of_category_in_live() {
        Position startPosition = eventSource.readAll().readAllForwards().limit(4).collect(toList()).get(3).position();

        List<NewEvent> readEvents = eventSource.readCategory().readCategoryForwards("category_1", startPosition)
                .map(ResolvedEvent::eventRecord)
                .map(p -> newEvent(p.eventType(), p.data(), p.metadata()))
                .collect(toList());

        assertThat(readEvents, contains(
                event("L3")
        ));
    }

    @Test
    public void
    can_continue_reading_from_position_at_end_of_category() {
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        Position position = eventSource.readCategory().readCategoryForwards("category_2").reduce((a, b) -> b).get().position();

        assertThat(eventSource.readCategory().readCategoryForwards("category_2", position).collect(toList()), empty());
    }

    private NewEvent event(String eventType) {
        return newEvent(
                eventType,
                eventType.getBytes(UTF_8),
                new StringBuilder(eventType).reverse().toString().getBytes(UTF_8)
        );
    }

}
