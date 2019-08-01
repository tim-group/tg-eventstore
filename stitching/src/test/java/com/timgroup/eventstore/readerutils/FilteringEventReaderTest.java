package com.timgroup.eventstore.readerutils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.timgroup.clocks.testing.ManualClock;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.EventRecordMatcher;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.memory.InMemoryEventSource;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.timgroup.eventstore.api.EventRecordMatcher.anEventRecord;
import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public final class FilteringEventReaderTest {

    private final ManualClock clock = new ManualClock(Instant.parse("2009-04-12T22:12:32Z"), ZoneOffset.UTC);
    private final JavaInMemoryEventStore inputReader = new JavaInMemoryEventStore(clock);
    private final InMemoryEventSource inputSource = new InMemoryEventSource(inputReader);

    @Test public void
    it_lets_everything_through_with_always_true_predicate() throws Exception {
        inputEventArrived(streamId("david", "tom"), newEvent("CoolenessAdded", new byte[0], new byte[0]));
        inputEventArrived(streamId("david", "tom"), newEvent("CoolenessChanged", new byte[0], new byte[0]));
        inputEventArrived(streamId("foo", "bar"), newEvent("CoolenessRemoved", new byte[0], new byte[0]));

        Predicate<? super ResolvedEvent> predicate = (e -> true);

        FilteringEventReader underTest = new FilteringEventReader(inputSource.readAll(), predicate);

        EventRecordMatcher coolenessAdded = anEventRecord(
                clock.instant(),
                StreamId.streamId("david", "tom"),
                0L,
                "CoolenessAdded",
                new byte[0],
                new byte[0]
        );
        EventRecordMatcher coolenessChanged = anEventRecord(
                clock.instant(),
                StreamId.streamId("david", "tom"),
                1L,
                "CoolenessChanged",
                new byte[0],
                new byte[0]
        );
        EventRecordMatcher coolenessRemoved = anEventRecord(
                clock.instant(),
                StreamId.streamId("foo", "bar"),
                0L,
                "CoolenessRemoved",
                new byte[0],
                new byte[0]
        );

        List<EventRecord> eventsReadForwards = underTest.readAllForwards().map(ResolvedEvent::eventRecord).collect(toList());
        assertThat(eventsReadForwards, contains(coolenessAdded, coolenessChanged, coolenessRemoved));

        List<EventRecord> eventsReadBackwards = underTest.readAllBackwards().map(ResolvedEvent::eventRecord).collect(toList());
        assertThat(eventsReadBackwards, contains(coolenessRemoved, coolenessChanged, coolenessAdded));

    }

    @Test public void
    skips_filtered_events() throws Exception {
        inputEventArrived(streamId("david", "tom"), newEvent("CoolenessAdded", new byte[0], new byte[0]));
        inputEventArrived(streamId("david", "tom"), newEvent("CoolenessChanged", new byte[0], new byte[0]));
        inputEventArrived(streamId("foo", "bar"), newEvent("CoolenessRemoved", new byte[0], new byte[0]));

        Predicate<? super ResolvedEvent> predicate = (e -> "CoolenessRemoved".equals(e.eventRecord().eventType()));

        FilteringEventReader underTest = new FilteringEventReader(inputSource.readAll(), predicate);

        EventRecordMatcher coolenessRemoved = anEventRecord(
                clock.instant(),
                StreamId.streamId("foo", "bar"),
                0L,
                "CoolenessRemoved",
                new byte[0],
                new byte[0]
        );

        List<EventRecord> events = underTest.readAllForwards().map(ResolvedEvent::eventRecord).collect(toList());
        assertThat(events, contains(coolenessRemoved));

        List<EventRecord> eventsReadBackwards = underTest.readAllBackwards().map(ResolvedEvent::eventRecord).collect(toList());
        assertThat(eventsReadBackwards, contains(coolenessRemoved));
    }

    @Test public void
    returns_last_event_from_filtered_events() throws Exception {
        inputEventArrived(streamId("david", "tom"), newEvent("CoolenessAdded", new byte[0], new byte[0]));
        inputEventArrived(streamId("david", "tom"), newEvent("CoolenessChanged", new byte[0], new byte[0]));
        inputEventArrived(streamId("foo", "bar"), newEvent("CoolenessRemoved", new byte[0], new byte[0]));

        Predicate<? super ResolvedEvent> predicate = (e -> "CoolenessRemoved".equals(e.eventRecord().eventType()));

        FilteringEventReader underTest = new FilteringEventReader(inputSource.readAll(), predicate);

        EventRecordMatcher coolenessRemoved = anEventRecord(
                clock.instant(),
                StreamId.streamId("foo", "bar"),
                0L,
                "CoolenessRemoved",
                new byte[0],
                new byte[0]
        );

        Optional<EventRecord> eventsReadBackwards = underTest.readLastEvent().map(ResolvedEvent::eventRecord);
        assertThat(eventsReadBackwards.get(), is(coolenessRemoved));
    }

    @Test public void
    continuing_from_a_position_works() throws Exception {
        inputEventArrived(streamId("david", "tom"), newEvent("CoolenessAdded", new byte[0], new byte[0]));
        inputEventArrived(streamId("david", "tom"), newEvent("CoolenessChanged", new byte[0], new byte[0]));
        inputEventArrived(streamId("foo", "bar"), newEvent("CoolenessRemoved", new byte[0], new byte[0]));

        FilteringEventReader underTest = FilteringEventReader.containingEventTypes(inputSource.readAll(), Sets.newHashSet("CoolenessRemoved"));

        List<ResolvedEvent> events = underTest.readAllForwards().collect(toList());

        Position checkpoint = events.get(0).position();

        inputEventArrived(streamId("david", "tom"), newEvent("CoolenessAdded", new byte[0], new byte[0]));
        inputEventArrived(streamId("david", "tom"), newEvent("CoolenessChanged", new byte[0], new byte[0]));
        inputEventArrived(streamId("fizz", "bar"), newEvent("CoolenessRemoved", new byte[0], new byte[0]));

        List<ResolvedEvent> events2 = underTest.readAllForwards(checkpoint).collect(toList());
        assertThat(events2.stream().map(ResolvedEvent::eventRecord).collect(toList()), contains(
                anEventRecord(
                        clock.instant(),
                        StreamId.streamId("fizz", "bar"),
                        0L,
                        "CoolenessRemoved",
                        new byte[0],
                        new byte[0]
                )
        ));

        Position latestCoolenessRemovedPosition = events2.get(0).position();
        List<EventRecord> eventReadBackwards = underTest.readAllBackwards(latestCoolenessRemovedPosition).map(ResolvedEvent::eventRecord).collect(toList());
        assertThat(eventReadBackwards, contains(
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

    private void inputEventArrived(StreamId streamId, NewEvent... events) {
        inputReader.write(streamId, Lists.newArrayList(events));
    }

}