package com.timgroup.eventstore.diffing;

import com.google.common.collect.ImmutableList;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import org.junit.Test;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.timgroup.eventstore.api.EventRecordMatcher.anEventRecord;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

public class EventStreamDifferTest {

    private final JavaInMemoryEventStore eventStoreA = new JavaInMemoryEventStore(Clock.systemUTC());
    private final JavaInMemoryEventStore eventStoreB = new JavaInMemoryEventStore(Clock.systemUTC());

    private final StreamId streamA = StreamId.streamId("test", "streamA");
    private final StreamId streamB = StreamId.streamId("test", "streamB");

    private final CapturingDiffListener diffResults = new CapturingDiffListener();
    private final EventStreamDiffer differ = new EventStreamDiffer(diffResults);

    @Test public void
    reports_events_with_equal_type_and_effectiveTimestamp_and_data_as_matching() {
        eventStoreA.write(streamA, ImmutableList.of(
                event("type1", "data1", "{\"effective_timestamp\":\"TS1\",\"foo\":1}"),
                event("type2", "data2", "{\"bar\":2,\"effective_timestamp\":\"TS2\"}"),
                event("type3", "data3", "{\"baz\":3}")
        ));
        eventStoreB.write(streamB, ImmutableList.of(
                event("type1", "data1", "{\"effective_timestamp\":\"TS1\",\"foo\":666}"),
                event("type2", "data2", "{\"effective_timestamp\":\"TS2\"}"),
                event("type3", "data3", "")
        ));

        differ.diff(eventStoreA.readAllForwards(), eventStoreB.readAllForwards());

        assertThat(diffResults.matchingEvents, contains(
            anEventRecord(streamA, 0, "type1", "data1", "{\"effective_timestamp\":\"TS1\",\"foo\":1}"),
            anEventRecord(streamB, 0, "type1", "data1", "{\"effective_timestamp\":\"TS1\",\"foo\":666}"),
            anEventRecord(streamA, 1, "type2", "data2", "{\"bar\":2,\"effective_timestamp\":\"TS2\"}"),
            anEventRecord(streamB, 1, "type2", "data2", "{\"effective_timestamp\":\"TS2\"}"),
            anEventRecord(streamA, 2, "type3", "data3", "{\"baz\":3}"),
            anEventRecord(streamB, 2, "type3", "data3", "")
        ));
        assertThat(diffResults.differingEvents, empty());
        assertThat(diffResults.unmatchedEventsInA, empty());
        assertThat(diffResults.unmatchedEventsInB, empty());
    }

    @Test public void
    reports_events_with_equal_type_and_effectiveTimestamp_but_different_data_as_differing() {
        eventStoreA.write(streamA, ImmutableList.of(
                event("type1", "data1.1", "{\"effective_timestamp\":\"TS1\"}"),
                event("type2", "data2.1", "no timestamp")
        ));
        eventStoreB.write(streamB, ImmutableList.of(
                event("type1", "data1.2", "{\"effective_timestamp\":\"TS1\"}"),
                event("type2", "data2.2", "no timestamp either")
        ));

        differ.diff(eventStoreA.readAllForwards(), eventStoreB.readAllForwards());

        assertThat(diffResults.differingEvents, contains(
                anEventRecord(streamA, 0, "type1", "data1.1", "{\"effective_timestamp\":\"TS1\"}"),
                anEventRecord(streamB, 0, "type1", "data1.2", "{\"effective_timestamp\":\"TS1\"}"),
                anEventRecord(streamA, 1, "type2", "data2.1", "no timestamp"),
                anEventRecord(streamB, 1, "type2", "data2.2", "no timestamp either")
        ));
        assertThat(diffResults.matchingEvents, empty());
        assertThat(diffResults.unmatchedEventsInA, empty());
        assertThat(diffResults.unmatchedEventsInB, empty());
    }

    @Test public void
    reports_events_without_equal_type_and_effectiveTimestamp_as_unmatched() {
        eventStoreA.write(streamA, ImmutableList.of(
                event("type1", "data", "{\"effective_timestamp\":\"TS1.1\"}"),
                event("type2.1", "data", "{\"effective_timestamp\":\"TS2\"}")
        ));
        eventStoreB.write(streamB, ImmutableList.of(
                event("type1", "data", "{\"effective_timestamp\":\"TS1.2\"}"),
                event("type2.2", "data", "{\"effective_timestamp\":\"TS2\"}")
        ));

        differ.diff(eventStoreA.readAllForwards(), eventStoreB.readAllForwards());

        assertThat(diffResults.unmatchedEventsInA, contains(
                anEventRecord(streamA, 0, "type1", "data", "{\"effective_timestamp\":\"TS1.1\"}"),
                anEventRecord(streamA, 1, "type2.1", "data", "{\"effective_timestamp\":\"TS2\"}")
        ));
        assertThat(diffResults.unmatchedEventsInB, contains(
                anEventRecord(streamB, 0, "type1", "data", "{\"effective_timestamp\":\"TS1.2\"}"),
                anEventRecord(streamB, 1, "type2.2", "data", "{\"effective_timestamp\":\"TS2\"}")
        ));
        assertThat(diffResults.matchingEvents, empty());
        assertThat(diffResults.differingEvents, empty());
    }

    @Test public void
    reports_additional_events_at_the_end_of_streamA_as_unmatched() {
        eventStoreA.write(streamA, ImmutableList.of(
                event("common", "common", ""),
                event("additional1", "additional1", ""),
                event("additional2", "additional2", "")
        ));
        eventStoreB.write(streamB, ImmutableList.of(
                event("common", "common", "")
        ));

        differ.diff(eventStoreA.readAllForwards(), eventStoreB.readAllForwards());

        assertThat(diffResults.matchingEvents, contains(
                anEventRecord(streamA, 0, "common", "common", ""),
                anEventRecord(streamB, 0, "common", "common", "")
        ));
        assertThat(diffResults.differingEvents, empty());
        assertThat(diffResults.unmatchedEventsInA, contains(
                anEventRecord(streamA, 1, "additional1", "additional1", ""),
                anEventRecord(streamA, 2, "additional2", "additional2", "")
        ));
        assertThat(diffResults.unmatchedEventsInB, empty());
    }

    @Test public void
    reports_additional_events_at_the_end_of_streamB_as_unmatched() {
        eventStoreA.write(streamA, ImmutableList.of(
                event("common", "common", "")
        ));
        eventStoreB.write(streamB, ImmutableList.of(
                event("common", "common", ""),
                event("additional1", "additional1", ""),
                event("additional2", "additional2", "")
        ));

        differ.diff(eventStoreA.readAllForwards(), eventStoreB.readAllForwards());

        assertThat(diffResults.matchingEvents, contains(
                anEventRecord(streamA, 0, "common", "common", ""),
                anEventRecord(streamB, 0, "common", "common", "")
        ));
        assertThat(diffResults.differingEvents, empty());
        assertThat(diffResults.unmatchedEventsInA, empty());
        assertThat(diffResults.unmatchedEventsInB, contains(
                anEventRecord(streamB, 1, "additional1", "additional1", ""),
                anEventRecord(streamB, 2, "additional2", "additional2", "")
        ));
    }

    @Test public void
    continues_matching_events_after_intermittent_additional_events_if_all_are_in_effectiveTimestamp_order() {
        eventStoreA.write(streamA, ImmutableList.of(
                event("common1", "common1", "{\"effective_timestamp\":\"TS1\"}"),
                event("common2", "common2", "{\"effective_timestamp\":\"TS3\"}")
        ));
        eventStoreB.write(streamB, ImmutableList.of(
                event("common1", "common1", "{\"effective_timestamp\":\"TS1\"}"),
                event("additional1", "additional1", "{\"effective_timestamp\":\"TS2\"}"),
                event("common2", "common2", "{\"effective_timestamp\":\"TS3\"}")
        ));

        differ.diff(eventStoreA.readAllForwards(), eventStoreB.readAllForwards());

        assertThat(diffResults.matchingEvents, contains(
                anEventRecord(streamA, 0, "common1", "common1", "{\"effective_timestamp\":\"TS1\"}"),
                anEventRecord(streamB, 0, "common1", "common1", "{\"effective_timestamp\":\"TS1\"}"),
                anEventRecord(streamA, 1, "common2", "common2", "{\"effective_timestamp\":\"TS3\"}"),
                anEventRecord(streamB, 2, "common2", "common2", "{\"effective_timestamp\":\"TS3\"}")
        ));
        assertThat(diffResults.differingEvents, empty());
        assertThat(diffResults.unmatchedEventsInA, empty());
        assertThat(diffResults.unmatchedEventsInB, contains(
                anEventRecord(streamB, 1, "additional1", "additional1", "{\"effective_timestamp\":\"TS2\"}")
        ));
    }

    @Test public void
    cannot_match_all_events_when_they_are_out_of_effectiveTimestamp_order() {
        eventStoreA.write(streamA, ImmutableList.of(
                event("common-later", "data", "{\"effective_timestamp\":\"TS2\"}"),
                event("common-earlier", "data", "{\"effective_timestamp\":\"TS1\"}")
        ));
        eventStoreB.write(streamB, ImmutableList.of(
                event("common-earlier", "data", "{\"effective_timestamp\":\"TS1\"}"),
                event("common-later", "data", "{\"effective_timestamp\":\"TS2\"}")
        ));

        differ.diff(eventStoreA.readAllForwards(), eventStoreB.readAllForwards());

        assertThat(diffResults.matchingEvents, contains(
                anEventRecord(streamA, 0, "common-later", "data", "{\"effective_timestamp\":\"TS2\"}"),
                anEventRecord(streamB, 1, "common-later", "data", "{\"effective_timestamp\":\"TS2\"}")
        ));
        assertThat(diffResults.differingEvents, empty());
        assertThat(diffResults.unmatchedEventsInA, contains(
                anEventRecord(streamA, 1, "common-earlier", "data", "{\"effective_timestamp\":\"TS1\"}")
        ));
        assertThat(diffResults.unmatchedEventsInB, contains(
                anEventRecord(streamB, 0, "common-earlier", "data", "{\"effective_timestamp\":\"TS1\"}")
        ));
    }

    private static NewEvent event(String type, String data, String metadata) {
        return NewEvent.newEvent(type, data.getBytes(UTF_8), metadata.getBytes(UTF_8));
    }


    private static final class CapturingDiffListener implements DiffListener {
        public final List<EventRecord> matchingEvents = new ArrayList<>();
        public final List<EventRecord> differingEvents = new ArrayList<>();
        public final List<EventRecord> unmatchedEventsInA = new ArrayList<>();
        public final List<EventRecord> unmatchedEventsInB = new ArrayList<>();

        @Override public void onMatchingEvents(ResolvedEvent eventInStreamA, ResolvedEvent eventInStreamB) {
            matchingEvents.addAll(Arrays.asList(eventInStreamA.eventRecord(), eventInStreamB.eventRecord()));
        }
        @Override public void onDifferingEvents(ResolvedEvent eventInStreamA, ResolvedEvent eventInStreamB) {
            differingEvents.addAll(Arrays.asList(eventInStreamA.eventRecord(), eventInStreamB.eventRecord()));
        }
        @Override public void onUnmatchedEventInStreamA(ResolvedEvent eventInStreamA) {
            unmatchedEventsInA.add(eventInStreamA.eventRecord());
        }
        @Override public void onUnmatchedEventInStreamB(ResolvedEvent eventInStreamB) {
            unmatchedEventsInB.add(eventInStreamB.eventRecord());
        }
    }
}
