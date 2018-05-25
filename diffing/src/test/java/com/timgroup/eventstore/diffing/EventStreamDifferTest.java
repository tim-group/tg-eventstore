package com.timgroup.eventstore.diffing;

import com.google.common.collect.ImmutableList;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.diffing.listeners.DiffListener;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import org.junit.Test;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

public final class EventStreamDifferTest {

    private final JavaInMemoryEventStore eventStoreA = new JavaInMemoryEventStore(Clock.systemUTC());
    private final JavaInMemoryEventStore eventStoreB = new JavaInMemoryEventStore(Clock.systemUTC());

    private final StreamId streamA = StreamId.streamId("test", "streamA");
    private final StreamId streamB = StreamId.streamId("test", "streamB");

    private final CapturingDiffListener capturingListener = new CapturingDiffListener();

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

        new EventStreamDiffer(capturingListener).diff(eventStoreA.readAllForwards(), eventStoreB.readAllForwards());

        assertThat(capturingListener.results, contains(
                new Matching(diffEvent("TS1", "type1", "data1")),
                new Matching(diffEvent("TS2", "type2", "data2")),
                new Matching(diffEvent("", "type3", "data3"))
        ));
    }

    @Test public void
    reports_events_with_two_equal_properties_as_similar() {
        eventStoreA.write(streamA, ImmutableList.of(
                event("type1", "data1.1", "{\"effective_timestamp\":\"TS1\"}"),
                event("type2", "data2", "{\"effective_timestamp\":\"TS2.1\"}"),
                event("type3.1", "data3", "no timestamp")
        ));
        eventStoreB.write(streamB, ImmutableList.of(
                event("type1", "data1.2", "{\"effective_timestamp\":\"TS1\"}"), // same type and timestamp, different data
                event("type2", "data2", "{\"effective_timestamp\":\"TS2.2\"}"), // same type and data, different timestamp
                event("type3.2", "data3", "no timestamp either")                // same data and timestamp, different type
        ));

        new EventStreamDiffer(capturingListener).diff(eventStoreA.readAllForwards(), eventStoreB.readAllForwards());

        assertThat(capturingListener.results, contains(
                new Similar(diffEvent("TS1", "type1", "data1.1"), diffEvent("TS1", "type1", "data1.2")),
                new Similar(diffEvent("TS2.1", "type2", "data2"), diffEvent("TS2.2", "type2", "data2")),
                new Similar(diffEvent("", "type3.1", "data3"), diffEvent("", "type3.2", "data3"))
        ));
    }

    @Test public void
    reports_events_with_one_or_zero_equal_properties_as_unmatched() {
        eventStoreA.write(streamA, ImmutableList.of(
                event("type", "data", "{\"effective_timestamp\":\"TS1\"}"),
                event("type3", "data3", "{\"effective_timestamp\":\"TS2\"}")
        ));
        eventStoreB.write(streamB, ImmutableList.of(
                event("type", "data2", "{\"effective_timestamp\":\"TS0\"}"), // only type equal
                event("type2", "data", "{\"effective_timestamp\":\"TS0\"}"), // only data equal
                event("type2", "data2", "{\"effective_timestamp\":\"TS1\"}") // only timestamp equal

        ));

        new EventStreamDiffer(capturingListener).diff(eventStoreA.readAllForwards(), eventStoreB.readAllForwards());

        assertThat(capturingListener.results, contains(
                new UnmatchedInB(diffEvent("TS0", "type", "data2")),
                new UnmatchedInB(diffEvent("TS0", "type2", "data")),
                new UnmatchedInA(diffEvent("TS1", "type", "data")),
                new UnmatchedInB(diffEvent("TS1", "type2", "data2")),
                new UnmatchedInA(diffEvent("TS2", "type3", "data3"))
        ));
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

        new EventStreamDiffer(capturingListener).diff(eventStoreA.readAllForwards(), eventStoreB.readAllForwards());

        assertThat(capturingListener.results, contains(
                new Matching(diffEvent("", "common", "common")),
                new UnmatchedInA(diffEvent("", "additional1", "additional1")),
                new UnmatchedInA(diffEvent("", "additional2", "additional2"))
        ));
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

        new EventStreamDiffer(capturingListener).diff(eventStoreA.readAllForwards(), eventStoreB.readAllForwards());

        assertThat(capturingListener.results, contains(
                new Matching(diffEvent("", "common", "common")),
                new UnmatchedInB(diffEvent("", "additional1", "additional1")),
                new UnmatchedInB(diffEvent("", "additional2", "additional2"))
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

        new EventStreamDiffer(capturingListener).diff(eventStoreA.readAllForwards(), eventStoreB.readAllForwards());

        assertThat(capturingListener.results, contains(
                new Matching(diffEvent("TS1", "common1", "common1")),
                new UnmatchedInB(diffEvent("TS2", "additional1", "additional1")),
                new Matching(diffEvent("TS3", "common2", "common2"))
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

        new EventStreamDiffer(capturingListener).diff(eventStoreA.readAllForwards(), eventStoreB.readAllForwards());

        assertThat(capturingListener.results, contains(
                new UnmatchedInB(diffEvent("TS1", "common-earlier", "data")),
                new Matching(diffEvent("TS2", "common-later", "data")),
                new UnmatchedInA(diffEvent("TS1", "common-earlier", "data"))
        ));
    }

    private static NewEvent event(String type, String data, String metadata) {
        return NewEvent.newEvent(type, data.getBytes(UTF_8), metadata.getBytes(UTF_8));
    }

    private static DiffEvent diffEvent(String effectiveTimestamp, String type, String body) {
        return new DiffEvent(effectiveTimestamp, type, body.getBytes(UTF_8), null);
    }

    private static final class CapturingDiffListener implements DiffListener {
        public final List<CapturedCallback> results = new ArrayList<>();

        @Override public void onMatchingEvents(DiffEvent eventInStreamA, DiffEvent eventInStreamB) {
            results.add(new Matching(eventInStreamA, eventInStreamB));
        }
        @Override public void onSimilarEvents(DiffEvent eventInStreamA, DiffEvent eventInStreamB) {
            results.add(new Similar(eventInStreamA, eventInStreamB));
        }
        @Override public void onUnmatchedEventInStreamA(DiffEvent eventInStreamA) {
            results.add(new UnmatchedInA(eventInStreamA));
        }
        @Override public void onUnmatchedEventInStreamB(DiffEvent eventInStreamB) {
            results.add(new UnmatchedInB(eventInStreamB));
        }
    }
    private interface CapturedCallback {}
    private static final class Matching implements CapturedCallback {
        private final DiffEvent fromA;
        private final DiffEvent fromB;
        Matching(DiffEvent fromA, DiffEvent fromB) {
            this.fromA = fromA;
            this.fromB = fromB;
        }
        Matching(DiffEvent expected) {
            this.fromA = expected;
            this.fromB = expected;
        }
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Matching that = (Matching) o;
            return Objects.equals(fromA, that.fromA) &&
                    Objects.equals(fromB, that.fromB);
        }
        @Override public int hashCode() { return Objects.hash(fromA, fromB); }
        @Override public String toString() { return "matching - " + fromA + " / " + fromB; }
    }
    private static final class Similar implements CapturedCallback {
        private final DiffEvent fromA;
        private final DiffEvent fromB;
        Similar(DiffEvent fromA, DiffEvent fromB) {
            this.fromA = fromA;
            this.fromB = fromB;
        }
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Similar that = (Similar) o;
            return Objects.equals(fromA, that.fromA) &&
                    Objects.equals(fromB, that.fromB);
        }
        @Override public int hashCode() { return Objects.hash(fromA, fromB); }
        @Override public String toString() { return "similar - " + fromA + " / " + fromB; }
    }
    private static final class UnmatchedInA implements CapturedCallback {
        private final DiffEvent fromA;
        UnmatchedInA(DiffEvent fromA) {
            this.fromA = fromA;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            UnmatchedInA that = (UnmatchedInA) o;
            return Objects.equals(fromA, that.fromA);
        }
        @Override public int hashCode() { return Objects.hash(fromA); }
        @Override public String toString() { return "unmatched in B - " + fromA; }
    }
    private static final class UnmatchedInB implements CapturedCallback {
        private final DiffEvent fromB;
        UnmatchedInB(DiffEvent fromB) {
            this.fromB = fromB;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            UnmatchedInB that = (UnmatchedInB) o;
            return Objects.equals(fromB, that.fromB);
        }
        @Override public int hashCode() { return Objects.hash(fromB); }
        @Override public String toString() { return "unmatched in B - " + fromB; }
    }
}
