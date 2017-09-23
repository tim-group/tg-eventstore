package com.timgroup.eventstore.diffing;

import com.google.common.collect.ImmutableList;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import org.junit.Test;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

public class EventStreamDifferTest {

    private final JavaInMemoryEventStore eventStoreA = new JavaInMemoryEventStore(Clock.systemUTC());
    private final JavaInMemoryEventStore eventStoreB = new JavaInMemoryEventStore(Clock.systemUTC());

    private final StreamId streamA = StreamId.streamId("test", "streamA");
    private final StreamId streamB = StreamId.streamId("test", "streamB");

    private final CapturingDiffListener capturingListener = new CapturingDiffListener();
    private final EventStreamDiffer differ = new EventStreamDiffer(capturingListener);

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

        assertThat(capturingListener.results, contains(
            matching("type1", "data1", "TS1"),
            matching("type2", "data2", "TS2"),
            matching("type3", "data3", "")
        ));
    }

    @Test public void
    reports_events_with_equal_type_and_effectiveTimestamp_but_different_data_as_similar() {
        eventStoreA.write(streamA, ImmutableList.of(
                event("type1", "data1.1", "{\"effective_timestamp\":\"TS1\"}"),
                event("type2", "data2.1", "no timestamp")
        ));
        eventStoreB.write(streamB, ImmutableList.of(
                event("type1", "data1.2", "{\"effective_timestamp\":\"TS1\"}"),
                event("type2", "data2.2", "no timestamp either")
        ));

        differ.diff(eventStoreA.readAllForwards(), eventStoreB.readAllForwards());

        assertThat(capturingListener.results, contains(
                similar("type1", "data1.1", "TS1",
                                       "data1.2", "TS1"),
                similar("type2", "data2.1", "",
                                       "data2.2", "")
        ));
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

        assertThat(capturingListener.results, contains(
                unmatchedInA("type1", "data", "TS1.1"),
                unmatchedInB("type1", "data", "TS1.2"),
                unmatchedInA("type2.1", "data", "TS2"),
                unmatchedInB("type2.2", "data", "TS2")
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

        differ.diff(eventStoreA.readAllForwards(), eventStoreB.readAllForwards());

        assertThat(capturingListener.results, contains(
                matching("common", "common", ""),
                unmatchedInA("additional1", "additional1", ""),
                unmatchedInA( "additional2", "additional2", "")
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

        differ.diff(eventStoreA.readAllForwards(), eventStoreB.readAllForwards());

        assertThat(capturingListener.results, contains(
                matching( "common", "common", ""),
                unmatchedInB("additional1", "additional1", ""),
                unmatchedInB("additional2", "additional2", "")
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

        assertThat(capturingListener.results, contains(
                matching("common1", "common1", "TS1"),
                unmatchedInB("additional1", "additional1", "TS2"),
                matching("common2", "common2", "TS3")
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

        assertThat(capturingListener.results, contains(
                unmatchedInB("common-earlier", "data", "TS1"),
                matching("common-later", "data", "TS2"),
                unmatchedInA("common-earlier", "data", "TS1")
        ));
    }

    private static NewEvent event(String type, String data, String metadata) {
        return NewEvent.newEvent(type, data.getBytes(UTF_8), metadata.getBytes(UTF_8));
    }
    private static CapturedMatching matching(String type, String data, String timestamp) {
        DiffEvent matchedEvent = new DiffEvent(timestamp, type, data.getBytes(UTF_8), null);
        return new CapturedMatching(matchedEvent, matchedEvent);
    }
    private static CapturedSimilar similar(String type, String data1, String timestamp1, String data2, String timestamp2) {
        DiffEvent similar1 = new DiffEvent(timestamp1, type, data1.getBytes(UTF_8), null);
        DiffEvent similar2 = new DiffEvent(timestamp2, type, data2.getBytes(UTF_8), null);
        return new CapturedSimilar(similar1, similar2);
    }
    private static CapturedUnmatchedInA unmatchedInA(String type, String data, String timestamp) {
        return new CapturedUnmatchedInA(new DiffEvent(timestamp, type, data.getBytes(UTF_8), null));
    }
    private static CapturedUnmatchedInB unmatchedInB(String type, String data, String timestamp) {
        return new CapturedUnmatchedInB(new DiffEvent(timestamp, type, data.getBytes(UTF_8), null));
    }


    private static final class CapturingDiffListener implements DiffListener {
        public final List<CapturedCallback> results = new ArrayList<>();

        @Override public void onMatchingEvents(DiffEvent eventInStreamA, DiffEvent eventInStreamB) {
            results.add(new CapturedMatching(eventInStreamA, eventInStreamB));
        }
        @Override public void onSimilarEvents(DiffEvent eventInStreamA, DiffEvent eventInStreamB) {
            results.add(new CapturedSimilar(eventInStreamA, eventInStreamB));
        }
        @Override public void onUnmatchedEventInStreamA(DiffEvent eventInStreamA) {
            results.add(new CapturedUnmatchedInA(eventInStreamA));
        }
        @Override public void onUnmatchedEventInStreamB(DiffEvent eventInStreamB) {
            results.add(new CapturedUnmatchedInB(eventInStreamB));
        }
    }
    private interface CapturedCallback {}
    private static final class CapturedMatching implements CapturedCallback {
        private final DiffEvent fromA;
        private final DiffEvent fromB;
        CapturedMatching(DiffEvent fromA, DiffEvent fromB) {
            this.fromA = fromA;
            this.fromB = fromB;
        }
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CapturedMatching that = (CapturedMatching) o;
            return Objects.equals(fromA, that.fromA) &&
                    Objects.equals(fromB, that.fromB);
        }
        @Override public int hashCode() { return Objects.hash(fromA, fromB); }
        @Override public String toString() { return "matching - " + fromA + " / " + fromB; }
    }
    private static final class CapturedSimilar implements CapturedCallback {
        private final DiffEvent fromA;
        private final DiffEvent fromB;
        CapturedSimilar(DiffEvent fromA, DiffEvent fromB) {
            this.fromA = fromA;
            this.fromB = fromB;
        }
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CapturedSimilar that = (CapturedSimilar) o;
            return Objects.equals(fromA, that.fromA) &&
                    Objects.equals(fromB, that.fromB);
        }
        @Override public int hashCode() { return Objects.hash(fromA, fromB); }
        @Override public String toString() { return "similar - " + fromA + " / " + fromB; }
    }
    private static final class CapturedUnmatchedInA implements CapturedCallback {
        private final DiffEvent fromA;
        CapturedUnmatchedInA(DiffEvent fromA) {
            this.fromA = fromA;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CapturedUnmatchedInA that = (CapturedUnmatchedInA) o;
            return Objects.equals(fromA, that.fromA);
        }
        @Override public int hashCode() { return Objects.hash(fromA); }
        @Override public String toString() { return "unmatched in B - " + fromA; }
    }
    private static final class CapturedUnmatchedInB implements CapturedCallback {
        private final DiffEvent fromB;
        CapturedUnmatchedInB(DiffEvent fromB) {
            this.fromB = fromB;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CapturedUnmatchedInB that = (CapturedUnmatchedInB) o;
            return Objects.equals(fromB, that.fromB);
        }
        @Override public int hashCode() { return Objects.hash(fromB); }
        @Override public String toString() { return "unmatched in B - " + fromB; }
    }
}
