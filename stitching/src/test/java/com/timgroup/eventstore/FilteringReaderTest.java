package com.timgroup.eventstore;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.timgroup.clocks.testing.ManualClock;
import com.timgroup.eventstore.api.*;
import com.timgroup.eventstore.memory.InMemoryEventSource;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import com.timgroup.eventstore.shovelling.EventShovel;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static com.timgroup.indicatorinputstreamwriter.EventRecordMatcher.anEventRecord;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

public final class FilteringReaderTest {

    private final ManualClock clock = new ManualClock(Instant.parse("2009-04-12T22:12:32Z"), ZoneId.of("UTC"));
    private final JavaInMemoryEventStore inputReader = new JavaInMemoryEventStore(clock);
    private final InMemoryEventSource inputSource = new InMemoryEventSource(inputReader);

    @Test public void
    it_lets_everything_through_with_always_true_predicate() throws Exception {
        inputEventArrived(streamId("david", "tom"), newEvent("CoolenessAdded", new byte[0], new byte[0]));
        inputEventArrived(streamId("david", "tom"), newEvent("CoolenessChanged", new byte[0], new byte[0]));
        inputEventArrived(streamId("foo", "bar"), newEvent("CoolenessRemoved", new byte[0], new byte[0]));

        Predicate<? super ResolvedEvent> predicate = (e -> true);

        FilteringReader underTest = new FilteringReader(inputSource.readAll(), predicate);

        List<EventRecord> events = underTest.readAllForwards().map(ResolvedEvent::eventRecord).collect(Collectors.toList());

        assertThat(events, contains(
                anEventRecord(
                        clock.instant(),
                        StreamId.streamId("david", "tom"),
                        0L,
                        "CoolenessAdded",
                        new byte[0],
                        new byte[0]
                ),
                anEventRecord(
                        clock.instant(),
                        StreamId.streamId("david", "tom"),
                        1L,
                        "CoolenessChanged",
                        new byte[0],
                        new byte[0]
                ),
                anEventRecord(
                        clock.instant(),
                        StreamId.streamId("foo", "bar"),
                        0L,
                        "CoolenessRemoved",
                        new byte[0],
                        new byte[0]
                )
        ));
    }

    @Test public void
    skips_filtered_events() throws Exception {
        inputEventArrived(streamId("david", "tom"), newEvent("CoolenessAdded", new byte[0], new byte[0]));
        inputEventArrived(streamId("david", "tom"), newEvent("CoolenessChanged", new byte[0], new byte[0]));
        inputEventArrived(streamId("foo", "bar"), newEvent("CoolenessRemoved", new byte[0], new byte[0]));

        Predicate<? super ResolvedEvent> predicate = (e -> "CoolenessRemoved".equals(e.eventRecord().eventType()));

        FilteringReader underTest = new FilteringReader(inputSource.readAll(), predicate);

        List<EventRecord> events = underTest.readAllForwards().map(ResolvedEvent::eventRecord).collect(Collectors.toList());

        assertThat(events, contains(
                anEventRecord(
                        clock.instant(),
                        StreamId.streamId("foo", "bar"),
                        0L,
                        "CoolenessRemoved",
                        new byte[0],
                        new byte[0]
                )
        ));
    }

    @Test public void
    continuing_from_a_position_works() throws Exception {
        inputEventArrived(streamId("david", "tom"), newEvent("CoolenessAdded", new byte[0], new byte[0]));
        inputEventArrived(streamId("david", "tom"), newEvent("CoolenessChanged", new byte[0], new byte[0]));
        inputEventArrived(streamId("foo", "bar"), newEvent("CoolenessRemoved", new byte[0], new byte[0]));

        FilteringReader underTest = FilteringReader.containingEventTypes(inputSource.readAll(), Sets.newHashSet("CoolenessRemoved"));

        List<ResolvedEvent> events = underTest.readAllForwards().collect(Collectors.toList());

        Position checkpoint = events.get(0).position();

        inputEventArrived(streamId("david", "tom"), newEvent("CoolenessAdded", new byte[0], new byte[0]));
        inputEventArrived(streamId("david", "tom"), newEvent("CoolenessChanged", new byte[0], new byte[0]));
        inputEventArrived(streamId("fizz", "bar"), newEvent("CoolenessRemoved", new byte[0], new byte[0]));

        List<EventRecord> events2 = underTest.readAllForwards(checkpoint).map(ResolvedEvent::eventRecord).collect(Collectors.toList());

        assertThat(events2, contains(
                anEventRecord(
                        clock.instant(),
                        StreamId.streamId("fizz", "bar"),
                        0L,
                        "CoolenessRemoved",
                        new byte[0],
                        new byte[0]
                )
        ));
    }

    private void inputEventArrived(StreamId streamId, NewEvent... events) {
        inputReader.write(streamId, Lists.newArrayList(events));
    }

}