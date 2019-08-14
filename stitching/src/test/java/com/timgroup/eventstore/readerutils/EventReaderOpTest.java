package com.timgroup.eventstore.readerutils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.timgroup.clocks.testing.ManualClock;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.memory.InMemoryEventSource;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import com.timgroup.eventstore.merging.MergingStrategy;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class EventReaderOpTest {
    @Test
    public void operation_can_be_applied_to_an_event_reader() {
        assertThat(addEventTypeSuffix("Foo").apply(eventStore("Alpha")), containsEventsWithTypes("AlphaFoo"));
    }

    @Test
    public void operation_can_be_applied_to_an_event_source() {
        assertThat(addEventTypeSuffix("Foo").apply(new InMemoryEventSource(eventStore("Alpha"))), containsEventsWithTypes("AlphaFoo"));
    }

    @Test
    public void flow_combines_reader_ops_in_sequence() {
        EventReaderOp combined = EventReaderOp.flow(
                addEventTypeSuffix("Foo"),
                addEventTypeSuffix("Bar")
        );
        assertThat(combined.apply(eventStore("Beta")), containsEventsWithTypes("BetaFooBar"));
    }

    @Test
    public void andThen_combines_reader_ops_in_sequence() {
        EventReaderOp combined = addEventTypeSuffix("Foo").andThen(addEventTypeSuffix("Bar"));
        assertThat(combined.apply(eventStore("Beta")), containsEventsWithTypes("BetaFooBar"));
    }

    @Test
    public void flow_with_zero_arguments_is_just_an_identity() {
        JavaInMemoryEventStore store = eventStore("Test");
        assertThat(EventReaderOp.flow().apply(store), sameInstance(store));
    }

    @Test
    public void filter_by_event_type() {
        assertThat(EventReaderOp.filterContainingEventTypes(ImmutableSet.of("RedEvent")).apply(eventStore("RedEvent", "BlueEvent")), containsEventsWithTypes("RedEvent"));
    }

    @Test
    public void filter_by_predicate() {
        List<String> filtered = new ArrayList<>();
        Predicate<ResolvedEvent> predicate = re -> {
            filtered.add(re.eventRecord().eventType());
            return true;
        };
        assertThat(EventReaderOp.filter(predicate).apply(eventStore("RedEvent", "BlueEvent")), containsEventsWithTypes("RedEvent", "BlueEvent"));
        assertThat(filtered, equalTo(ImmutableList.of("RedEvent", "BlueEvent")));
    }

    @Test
    public void backdate() {
        JavaInMemoryEventStore store = new JavaInMemoryEventStore(new ManualClock(Instant.EPOCH, ZoneOffset.UTC));
        store.write(streamId("any", "any"), ImmutableList.of(newEvent("EarlyEvent", new byte[0], "{\"effective_timestamp\":\"1994-01-01T00:00:00Z\"}".getBytes(UTF_8))));
        store.write(streamId("any", "any"), ImmutableList.of(newEvent("LateEvent", new byte[0], "{\"effective_timestamp\":\"2019-01-01T00:00:00Z\"}".getBytes(UTF_8))));

        assertThat(EventReaderOp.backdate(Instant.parse("2000-01-01T00:00:00Z"), Instant.parse("1984-01-01T00:00:00Z")).apply(store), containsEventsWithMetadata("{\"effective_timestamp\":\"1984-01-01T00:00:00Z\"}", "{\"effective_timestamp\":\"2019-01-01T00:00:00Z\"}"));
    }

    @Test
    public void rekey() {
        JavaInMemoryEventStore store = new JavaInMemoryEventStore(new ManualClock(Instant.EPOCH, ZoneOffset.UTC));
        store.write(streamId("any", "red"), ImmutableList.of(newEvent("RedEvent", new byte[0], new byte[0])));
        store.write(streamId("any", "blue"), ImmutableList.of(newEvent("BlueEvent", new byte[0], new byte[0])));

        assertThat(EventReaderOp.rekey(streamId("new", "new")).apply(eventStore("RedEvent", "BlueEvent")), containsEvents(
                eventRecord(
                        Instant.EPOCH,
                        streamId("new", "new"),
                        0L,
                        "RedEvent",
                        new byte[0],
                        new byte[0]
                ),
                eventRecord(
                        Instant.EPOCH,
                        streamId("new", "new"),
                        1L,
                        "BlueEvent",
                        new byte[0],
                        new byte[0]
                )
        ));
    }

    @Test
    public void transform_event_records() {
        List<EventRecord> transformed = new ArrayList<>();
        Iterator<EventRecord> trasnformedTo = ImmutableList.of(eventRecord(
                Instant.EPOCH,
                streamId("any", "any"),
                0L,
                "TransformedEvent",
                new byte[0],
                new byte[0]
        )).iterator();
        assertThat(EventReaderOp.transformEventRecords(e -> {
            transformed.add(e);
            return trasnformedTo.next();
        }).apply(eventStore("InputEvent")), containsEventsWithTypes("TransformedEvent"));
        assertThat(transformed.stream().map(EventRecord::eventType).collect(toList()), equalTo(ImmutableList.of("InputEvent")));
    }

    @Test
    public void transform_resolved_events() {
        List<ResolvedEvent> transformed = new ArrayList<>();
        Iterator<ResolvedEvent> trasnformedTo = ImmutableList.of(eventRecord(
                Instant.EPOCH,
                streamId("any", "any"),
                0L,
                "TransformedEvent",
                new byte[0],
                new byte[0]
        ).toResolvedEvent(new Position() {})).iterator();
        assertThat(EventReaderOp.transformResolvedEvents(re -> {
            transformed.add(re);
            return trasnformedTo.next();
        }).apply(eventStore("InputEvent")), containsEventsWithTypes("TransformedEvent"));
        assertThat(transformed.stream().map(re -> re.eventRecord().eventType()).collect(toList()), equalTo(ImmutableList.of("InputEvent")));
    }

    @Test
    public void exclude_events_written_before() {
        ManualClock clock = new ManualClock(Instant.EPOCH, ZoneOffset.UTC);
        JavaInMemoryEventStore store = new JavaInMemoryEventStore(clock);
        clock.advanceTo(Instant.parse("1984-01-01T00:00:00Z"));
        store.write(streamId("any", "red"), ImmutableList.of(newEvent("RedEvent", new byte[0], new byte[0])));
        clock.advanceTo(Instant.parse("2019-01-01T00:00:00Z"));
        store.write(streamId("any", "blue"), ImmutableList.of(newEvent("BlueEvent", new byte[0], new byte[0])));

        assertThat(EventReaderOp.excludeEventsWrittenBefore(Instant.parse("2000-01-01T00:00:00Z")).apply(store), containsEventsWithTypes("BlueEvent"));

    }

    @Test
    public void reorder() {
        ManualClock clock = new ManualClock(Instant.EPOCH, ZoneOffset.UTC);
        JavaInMemoryEventStore store = new JavaInMemoryEventStore(clock);
        clock.advanceTo(Instant.parse("1976-01-01T00:00:00Z"));
        store.write(streamId("any", "any"), ImmutableList.of(newEvent("LateEventWrittenEarly", new byte[0], "{\"effective_timestamp\":\"1984-01-01T00:00:00Z\"}".getBytes(UTF_8))));
        clock.advanceTo(Instant.parse("1984-01-01T00:00:00Z"));
        store.write(streamId("any", "any"), ImmutableList.of(newEvent("EarlyEventWrittenLate", new byte[0], "{\"effective_timestamp\":\"1976-01-01T00:00:00Z\"}".getBytes(UTF_8))));
        clock.advanceTo(Instant.parse("2019-01-01T00:00:00Z"));
        store.write(streamId("any", "any"), ImmutableList.of(newEvent("ReallyLateEvent", new byte[0], "{\"effective_timestamp\":\"2019-01-01T00:00:00Z\"}".getBytes(UTF_8))));

        assertThat(EventReaderOp.reorder(Instant.parse("2000-01-01T00:00:00Z"), new MergingStrategy.EffectiveTimestampMergingStrategy()::toComparable).apply(store), containsEventsWithTypes("EarlyEventWrittenLate", "LateEventWrittenEarly", "ReallyLateEvent"));

    }

    private static Matcher<EventReader> containsEvents(EventRecord... events) {
        return containsEvents(equalTo(Arrays.asList(events)));
    }

    private static Matcher<EventReader> containsEvents(Matcher<? super List<EventRecord>> contentsMatcher) {
        return new TypeSafeDiagnosingMatcher<EventReader>() {
            @Override
            protected boolean matchesSafely(EventReader item, Description mismatchDescription) {
                List<EventRecord> events = item.readAllForwards().map(ResolvedEvent::eventRecord).collect(toList());
                contentsMatcher.describeMismatch(events, mismatchDescription);
                return contentsMatcher.matches(events);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("contains events with types ").appendDescriptionOf(contentsMatcher);
            }
        };
    }

    private static Matcher<EventReader> containsEventsWithTypes(String... eventTypes) {
        return containsEventsWithTypes(equalTo(Arrays.asList(eventTypes)));
    }

    private static Matcher<EventReader> containsEventsWithTypes(Matcher<? super List<String>> contentsMatcher) {
        return new TypeSafeDiagnosingMatcher<EventReader>() {
            @Override
            protected boolean matchesSafely(EventReader item, Description mismatchDescription) {
                List<String> events = item.readAllForwards().map(re -> re.eventRecord().eventType()).collect(toList());
                contentsMatcher.describeMismatch(events, mismatchDescription);
                return contentsMatcher.matches(events);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("contains events with types ").appendDescriptionOf(contentsMatcher);
            }
        };
    }

    private static Matcher<EventReader> containsEventsWithMetadata(String... metadata) {
        return containsEventsWithMetadata(equalTo(Arrays.asList(metadata)));
    }

    private static Matcher<EventReader> containsEventsWithMetadata(Matcher<? super List<String>> contentsMatcher) {
        return new TypeSafeDiagnosingMatcher<EventReader>() {
            @Override
            protected boolean matchesSafely(EventReader item, Description mismatchDescription) {
                List<String> events = item.readAllForwards().map(re -> new String(re.eventRecord().metadata(), UTF_8)).collect(toList());
                contentsMatcher.describeMismatch(events, mismatchDescription);
                return contentsMatcher.matches(events);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("contains events with metadata ").appendDescriptionOf(contentsMatcher);
            }
        };
    }

    private static JavaInMemoryEventStore eventStore(String... eventTypes) {
        JavaInMemoryEventStore store = new JavaInMemoryEventStore(new ManualClock(Instant.EPOCH, ZoneOffset.UTC));
        store.write(streamId("any", "any"), Arrays.stream(eventTypes).map(t -> newEvent(t, new byte[0])).collect(toList()));
        return store;
    }

    private static EventReaderOp addEventTypeSuffix(String suffix) {
        return EventReaderOp.transformEventRecords(e -> eventRecord(
                e.timestamp(),
                e.streamId(),
                e.eventNumber(),
                e.eventType() + suffix,
                e.data(),
                e.metadata()
        ));
    }
}