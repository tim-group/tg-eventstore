package com.timgroup.eventstore.merging;

import com.timgroup.clocks.testing.ManualClock;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.memory.InMemoryEventSource;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;

import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static com.timgroup.indicatorinputstreamwriter.EventRecordMatcher.anEventRecord;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singleton;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

public final class EventShovelTest {

    private final ManualClock clock = new ManualClock(Instant.parse("2009-04-12T22:12:32Z"), ZoneId.of("UTC"));

    private final JavaInMemoryEventStore inputReader = new JavaInMemoryEventStore(clock);
    private final InMemoryEventSource inputSource = new InMemoryEventSource(inputReader);

    private final JavaInMemoryEventStore outputStore = new JavaInMemoryEventStore(clock);
    private final InMemoryEventSource outputSource = new InMemoryEventSource(outputStore);


    private final EventShovel underTest = new EventShovel(inputSource.readAll(), inputSource.positionCodec(), outputSource);

    @Test public void
    it_shovels_all_events() throws Exception {
        inputEventArrived(streamId("david", "tom"), newEvent("CoolenessAdded", new byte[0], new byte[0]));

        underTest.shovelAllNewlyAvailableEvents();

        List<EventRecord> shovelledEvents = outputSource.readAll().readAllForwards().map(ResolvedEvent::eventRecord).collect(Collectors.toList());

        assertThat(shovelledEvents, contains(
                anEventRecord(
                        clock.instant(),
                        StreamId.streamId("david", "tom"),
                        0L,
                        "CoolenessAdded",
                        new byte[0],
                        "{\"shovel_position\":\"1\"}".getBytes(UTF_8)
                )
        ));
    }

    @Test public void
    it_shovels_only_events_that_it_has_not_previously_shovelled() throws Exception {
        inputEventArrived(streamId("previous", "event"), newEvent("CoolenessRemoved", new byte[0], new byte[0]));
        underTest.shovelAllNewlyAvailableEvents();

        inputEventArrived(streamId("david", "tom"), newEvent("CoolenessAdded", new byte[0], new byte[0]));
        underTest.shovelAllNewlyAvailableEvents();

        List<EventRecord> shovelledEvents = outputSource.readAll().readAllForwards().skip(1).map(ResolvedEvent::eventRecord).collect(Collectors.toList());

        assertThat(shovelledEvents, contains(
                anEventRecord(
                        clock.instant(),
                        StreamId.streamId("david", "tom"),
                        0L,
                        "CoolenessAdded",
                        new byte[0],
                        "{\"shovel_position\":\"2\"}".getBytes(UTF_8)
                )
        ));
    }

    @Test public void
    a_new_shovel_shovels_only_events_that_it_has_not_previously_shovelled() throws Exception {
        inputEventArrived(streamId("previous", "event"), newEvent("CoolenessRemoved", new byte[0], new byte[0]));
        underTest.shovelAllNewlyAvailableEvents();

        inputEventArrived(streamId("david", "tom"), newEvent("CoolenessAdded", new byte[0], new byte[0]));
        new EventShovel(inputSource.readAll(), inputSource.positionCodec(), outputSource).shovelAllNewlyAvailableEvents();

        List<EventRecord> shovelledEvents = outputSource.readAll().readAllForwards().skip(1).map(ResolvedEvent::eventRecord).collect(Collectors.toList());

        assertThat(shovelledEvents, contains(
                anEventRecord(
                        clock.instant(),
                        StreamId.streamId("david", "tom"),
                        0L,
                        "CoolenessAdded",
                        new byte[0],
                        "{\"shovel_position\":\"2\"}".getBytes(UTF_8)
                )
        ));
    }

    @Test public void
    maintains_metadata_from_upstream() throws Exception {
        String originalMetadata = "{\"effective_timestamp\":\"2015-02-12T04:23:34Z\"}";
        inputEventArrived(streamId("david", "tom"), newEvent("CoolenessAdded", new byte[0], originalMetadata.getBytes(UTF_8)));

        underTest.shovelAllNewlyAvailableEvents();

        List<EventRecord> shovelledEvents = outputSource.readAll().readAllForwards().map(ResolvedEvent::eventRecord).collect(Collectors.toList());

        assertThat(shovelledEvents, contains(
                anEventRecord(
                        clock.instant(),
                        StreamId.streamId("david", "tom"),
                        0L,
                        "CoolenessAdded",
                        new byte[0],
                        "{\"effective_timestamp\":\"2015-02-12T04:23:34Z\",\"shovel_position\":\"1\"}".getBytes(UTF_8)
                )
        ));
    }

    private void inputEventArrived(StreamId streamId, NewEvent event) {
        inputReader.write(streamId, singleton(event));
    }

}