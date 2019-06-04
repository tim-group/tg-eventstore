package com.timgroup.eventstore.merging;

import com.google.common.collect.ImmutableList;
import com.timgroup.clocks.testing.ManualClock;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static com.timgroup.eventstore.api.EventRecordMatcher.anEventRecord;
import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.ofSeconds;
import static java.util.Collections.singleton;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

public final class MergedEventSourceTest {
    @Rule public final ExpectedException thrown = ExpectedException.none();

    private final ManualClock clock = new ManualClock(Instant.parse("2009-04-12T22:12:32Z"), ZoneId.of("UTC"));

    @Test public void
    supports_reading_all_forwards_from_a_single_input_stream() throws Exception {
        JavaInMemoryEventStore input = new JavaInMemoryEventStore(clock);

        EventReader outputReader = MergedEventSource.streamOrderMergedEventSource(clock, new NamedReaderWithCodec("a", input, JavaInMemoryEventStore.CODEC)).readAll();

        Instant instant = clock.instant();
        inputEventArrived(input, "CoolenessAdded");
        inputEventArrived(input, "CoolenessChanged");
        inputEventArrived(input, "CoolenessRemoved");

        clock.advanceTo(instant.plusSeconds(1));
        List<EventRecord> mergedEvents = outputReader.readAllForwards().map(ResolvedEvent::eventRecord).collect(Collectors.toList());

        assertThat(mergedEvents, contains(
                anEventRecord(
                        instant,
                        streamId("all", "all"),
                        0L,
                        "CoolenessAdded",
                        new byte[0],
                        new byte[0]
                ),
                anEventRecord(
                        instant,
                        streamId("all", "all"),
                        1L,
                        "CoolenessChanged",
                        new byte[0],
                        new byte[0]
                ),
                anEventRecord(
                        instant,
                        streamId("all", "all"),
                        2L,
                        "CoolenessRemoved",
                        new byte[0],
                        new byte[0]
                )
        ));
    }

    @Test public void
    does_not_skip_newer_events_for_out_of_order_older_events_when_applying_a_delay() throws Exception {
        Instant event1Instant = clock.instant();
        Instant event2Instant = event1Instant.minusSeconds(1L);

        EventReader input = new EventReader() {
            @Nonnull
            @CheckReturnValue
            @Override
            public Stream<ResolvedEvent> readAllForwards(@Nonnull Position positionExclusive) {
                return ImmutableList.of(
                        new ResolvedEvent(new Position() { }, eventRecord(event1Instant, streamId("a", "1"), 1L, "X", new byte[0], new byte[0])),
                        new ResolvedEvent(new Position() { }, eventRecord(event2Instant, streamId("b", "1"), 1L, "X", new byte[0], new byte[0]))
                ).stream();
            }

            @Nonnull
            @Override public Position emptyStorePosition() { return new Position() { }; }
        };

        EventReader outputReader = MergedEventSource.streamOrderMergedEventSource(clock, ofSeconds(1L), new NamedReaderWithCodec("a", input, JavaInMemoryEventStore.CODEC)).readAll();

        assertThat(outputReader.readAllForwards().count(), is(0L));

        clock.bump(ofSeconds(1L));
        assertThat(outputReader.readAllForwards().count(), is(2L));
    }

    @Test public void
    supports_reading_from_given_position_from_a_single_input_stream() throws Exception {
        JavaInMemoryEventStore input = new JavaInMemoryEventStore(clock);

        EventReader outputReader = MergedEventSource.streamOrderMergedEventSource(clock, new NamedReaderWithCodec("a", input, JavaInMemoryEventStore.CODEC)).readAll();

        Instant instant = clock.instant();
        inputEventArrived(input, "CoolenessAdded");
        inputEventArrived(input, "CoolenessChanged");

        clock.advanceTo(instant.plusSeconds(1));
        @SuppressWarnings("OptionalGetWithoutIsPresent") Position startPosition = outputReader.readAllForwards().findFirst().get().position();
        List<EventRecord> mergedEvents = outputReader.readAllForwards(startPosition).map(ResolvedEvent::eventRecord).collect(Collectors.toList());

        assertThat(mergedEvents, contains(
                anEventRecord(
                        instant,
                        streamId("all", "all"),
                        1L,
                        "CoolenessChanged",
                        new byte[0],
                        new byte[0]
                )
        ));
    }

    @Test public void
    supports_reading_all_forwards_from_multiple_input_streams_with_serialisation_of_position() throws Exception {
        JavaInMemoryEventStore input1 = new JavaInMemoryEventStore(clock);
        JavaInMemoryEventStore input2 = new JavaInMemoryEventStore(clock);
        MergedEventSource eventSource = MergedEventSource.streamOrderMergedEventSource(
                clock,
                new NamedReaderWithCodec("a", input1, JavaInMemoryEventStore.CODEC),
                new NamedReaderWithCodec("b", input2, JavaInMemoryEventStore.CODEC)
        );
        EventReader outputReader = eventSource.readAll();

        Instant instant1 = clock.instant();
        inputEventArrived(input1, streamId("all", "all"), "CoolenessAdded");
        inputEventArrived(input2, streamId("oh", "fook"), "CoolenessRemoved");
        inputEventArrived(input2, streamId("oh", "fook"), "CoolenessChanged");

        clock.advanceTo(instant1.plusSeconds(1));
        @SuppressWarnings("OptionalGetWithoutIsPresent") Position startPosition = outputReader.readAllForwards().skip(1).findFirst().get().position();
        Position deserialisedStartPosition = eventSource.positionCodec().deserializePosition(eventSource.positionCodec().serializePosition(startPosition));

        Instant instant2 = clock.instant();
        inputEventArrived(input1, streamId("all", "all"), "CoolenessDestroyed");

        clock.advanceTo(instant2.plusSeconds(1));
        List<EventRecord> mergedEvents = outputReader.readAllForwards(deserialisedStartPosition).map(ResolvedEvent::eventRecord).collect(Collectors.toList());

        assertThat(mergedEvents, contains(
                anEventRecord(
                        instant2,
                        streamId("all", "all"),
                        1L,
                        "CoolenessDestroyed",
                        new byte[0],
                        new byte[0]
                ),
                anEventRecord(
                        instant1,
                        streamId("oh", "fook"),
                        1L,
                        "CoolenessChanged",
                        new byte[0],
                        new byte[0]
                )
        ));
    }

    @Test public void
    supports_reading_from_a_given_position_forwards_from_multiple_input_streams() throws Exception {
        JavaInMemoryEventStore input1 = new JavaInMemoryEventStore(clock);
        JavaInMemoryEventStore input2 = new JavaInMemoryEventStore(clock);
        EventReader outputReader = MergedEventSource.streamOrderMergedEventSource(
                clock,
                new NamedReaderWithCodec("a", input1, JavaInMemoryEventStore.CODEC),
                new NamedReaderWithCodec("b", input2, JavaInMemoryEventStore.CODEC)
        ).readAll();

        Instant instant = clock.instant();
        inputEventArrived(input1, streamId("all", "all"), "CoolenessAdded");
        inputEventArrived(input2, streamId("oh", "fook"), "CoolenessRemoved");
        inputEventArrived(input1, streamId("all", "all"), "CoolenessChanged");

        clock.advanceTo(instant.plusSeconds(1));
        List<EventRecord> mergedEvents = outputReader.readAllForwards().map(ResolvedEvent::eventRecord).collect(Collectors.toList());

        assertThat(mergedEvents, contains(
                anEventRecord(
                        instant,
                        streamId("all", "all"),
                        0L,
                        "CoolenessAdded",
                        new byte[0],
                        new byte[0]
                ),
                anEventRecord(
                        instant,
                        streamId("all", "all"),
                        1L,
                        "CoolenessChanged",
                        new byte[0],
                        new byte[0]
                ),
                anEventRecord(
                        instant,
                        streamId("oh", "fook"),
                        0L,
                        "CoolenessRemoved",
                        new byte[0],
                        new byte[0]
                )
        ));
    }

    @Test public void
    supports_reading_all_forwards_from_multiple_input_streams_merging_by_effective_timestamp() throws Exception {
        JavaInMemoryEventStore input1 = new JavaInMemoryEventStore(clock);
        JavaInMemoryEventStore input2 = new JavaInMemoryEventStore(clock);

        EventReader outputReader = MergedEventSource.effectiveTimestampMergedEventSource(
                clock,
                Duration.ZERO,
                new NamedReaderWithCodec("a", input1, JavaInMemoryEventStore.CODEC),
                new NamedReaderWithCodec("b", input2, JavaInMemoryEventStore.CODEC)
        ).readAll();

        Instant instant = clock.instant();
        inputEventArrived(input1, streamId("baz", "bob"), newEvent("CoolenessAdded",   new byte[0], "{\"effective_timestamp\":\"2014-01-23T00:23:54Z\"}".getBytes(UTF_8)));
        inputEventArrived(input1, streamId("foo", "bar"), newEvent("CoolenessRemoved", new byte[0], "{\"effective_timestamp\":\"2016-01-23T00:23:54Z\"}".getBytes(UTF_8)));
        inputEventArrived(input2, streamId("arg", "erg"), newEvent("CoolenessChanged", new byte[0], "{\"effective_timestamp\":\"2015-01-23T00:23:54Z\"}".getBytes(UTF_8)));

        clock.advanceTo(instant.plusSeconds(1));
        List<EventRecord> mergedEvents = outputReader.readAllForwards().map(ResolvedEvent::eventRecord).collect(Collectors.toList());

        assertThat(mergedEvents, contains(
                anEventRecord(
                        instant,
                        streamId("baz", "bob"),
                        0L,
                        "CoolenessAdded",
                        new byte[0],
                        "{\"effective_timestamp\":\"2014-01-23T00:23:54Z\"}".getBytes(UTF_8)
                ),
                anEventRecord(
                        instant,
                        streamId("arg", "erg"),
                        0L,
                        "CoolenessChanged",
                        new byte[0],
                        "{\"effective_timestamp\":\"2015-01-23T00:23:54Z\"}".getBytes(UTF_8)
                ),
                anEventRecord(
                        instant,
                        streamId("foo", "bar"),
                        0L,
                        "CoolenessRemoved",
                        new byte[0],
                        "{\"effective_timestamp\":\"2016-01-23T00:23:54Z\"}".getBytes(UTF_8)
                )
        ));
    }

    @Test public void
    supports_reading_all_forwards_from_multiple_input_streams_where_not_all_input_streams_are_present_in_the_start_position() {
        JavaInMemoryEventStore input1 = new JavaInMemoryEventStore(clock);
        JavaInMemoryEventStore input2 = new JavaInMemoryEventStore(clock);

        MergedEventSource mergedEventSource = MergedEventSource.effectiveTimestampMergedEventSource(
                true,
                clock,
                Duration.ZERO,
                new NamedReaderWithCodec("a", input1, JavaInMemoryEventStore.CODEC),
                new NamedReaderWithCodec("b", input2, JavaInMemoryEventStore.CODEC)
        );

        Instant instant = clock.instant();
        inputEventArrived(input1, streamId("baz", "bob"), newEvent("CoolenessAdded",   new byte[0], "{\"effective_timestamp\":\"2014-01-23T00:23:54Z\"}".getBytes(UTF_8)));
        inputEventArrived(input1, streamId("foo", "bar"), newEvent("CoolenessRemoved", new byte[0], "{\"effective_timestamp\":\"2016-01-23T00:23:54Z\"}".getBytes(UTF_8)));

        clock.advanceTo(instant.plusSeconds(1));
        Position position = mergedEventSource.positionCodec().deserializePosition("{\"a\":\"1\"}");

        List<EventRecord> mergedEvents = mergedEventSource.readAll().readAllForwards(position).map(ResolvedEvent::eventRecord).collect(Collectors.toList());

        assertThat(mergedEvents, contains(
                anEventRecord(
                        instant,
                        streamId("foo", "bar"),
                        0L,
                        "CoolenessRemoved",
                        new byte[0],
                        "{\"effective_timestamp\":\"2016-01-23T00:23:54Z\"}".getBytes(UTF_8)
                )
        ));
    }
    @Test public void
    when_delay_is_set_to_zero_and_no_time_passes_still_allows_events_through() throws Exception {
        JavaInMemoryEventStore input1 = new JavaInMemoryEventStore(clock);

        EventReader outputReader = MergedEventSource.effectiveTimestampMergedEventSource(
                clock,
                Duration.ZERO,
                new NamedReaderWithCodec("a", input1, JavaInMemoryEventStore.CODEC)
        ).readAll();

        inputEventArrived(input1, streamId("baz", "bob"), newEvent("CoolenessAdded",   new byte[0], "{\"effective_timestamp\":\"2014-01-23T00:23:54Z\"}".getBytes(UTF_8)));

        assertThat(outputReader.readAllForwards().count(), is(1L));
    }

    @Test public void
    blows_up_when_merging_by_effective_timestamp_on_encountering_an_event_without_an_effective_timestamp() throws Exception {
        JavaInMemoryEventStore input1 = new JavaInMemoryEventStore(clock);

        EventReader outputReader = MergedEventSource.effectiveTimestampMergedEventSource(clock, new NamedReaderWithCodec("a", input1, JavaInMemoryEventStore.CODEC)).readAll();

        inputEventArrived(input1, streamId("baz", "bob"), newEvent("CoolenessAdded", new byte[0], "{\"cheese_date\":\"2014-01-23T00:23:54Z\"}".getBytes(UTF_8)));

        clock.advanceTo(clock.instant().plusSeconds(1));
        Iterator<ResolvedEvent> iterator = outputReader.readAllForwards().iterator();

        try {
            iterator.next();
            Assert.fail("expected IllegalStateException");
        } catch (IllegalStateException e) {
            //pass
        }
    }

    @Test public void
    has_readable_toString_for_its_position() throws Exception {
        JavaInMemoryEventStore input1 = new JavaInMemoryEventStore(clock);
        JavaInMemoryEventStore input2 = new JavaInMemoryEventStore(clock);
        MergedEventSource eventSource = MergedEventSource.streamOrderMergedEventSource(
                clock,
                new NamedReaderWithCodec("a", input1, JavaInMemoryEventStore.CODEC),
                new NamedReaderWithCodec("b", input2, JavaInMemoryEventStore.CODEC)
        );

        String positionToString = eventSource.readAll().emptyStorePosition().toString();
        assertThat(positionToString, is(equalTo("a:0;b:0")));
    }

    @Test public void
    blows_up_when_decoding_a_position_with_missing_feeds() throws Exception {
        JavaInMemoryEventStore input1 = new JavaInMemoryEventStore(clock);
        JavaInMemoryEventStore input2 = new JavaInMemoryEventStore(clock);
        MergedEventSource eventSource1 = MergedEventSource.streamOrderMergedEventSource(
                clock,
                new NamedReaderWithCodec("a", input1, JavaInMemoryEventStore.CODEC),
                new NamedReaderWithCodec("b", input2, JavaInMemoryEventStore.CODEC)
        );

        Position position = eventSource1.readAll().emptyStorePosition();
        String serialisedPosition = eventSource1.positionCodec().serializePosition(position);

        JavaInMemoryEventStore input3 = new JavaInMemoryEventStore(clock);
        MergedEventSource eventSource2 = MergedEventSource.streamOrderMergedEventSource(
                clock,
                new NamedReaderWithCodec("a", input1, JavaInMemoryEventStore.CODEC),
                new NamedReaderWithCodec("b", input2, JavaInMemoryEventStore.CODEC),
                new NamedReaderWithCodec("c", input3, JavaInMemoryEventStore.CODEC)
        );

        try {
            eventSource2.positionCodec().deserializePosition(serialisedPosition);
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Bad position, containing no key for c :{\"a\":\"0\",\"b\":\"0\"}"));
        }
    }

    @Test public void
    blows_up_when_decoding_a_position_with_extra_feeds() throws Exception {
        JavaInMemoryEventStore input1 = new JavaInMemoryEventStore(clock);
        JavaInMemoryEventStore input2 = new JavaInMemoryEventStore(clock);
        MergedEventSource eventSource1 = MergedEventSource.streamOrderMergedEventSource(
                clock,
                new NamedReaderWithCodec("a", input1, JavaInMemoryEventStore.CODEC),
                new NamedReaderWithCodec("b", input2, JavaInMemoryEventStore.CODEC)
        );

        Position position = eventSource1.readAll().emptyStorePosition();
        String serialisedPosition = eventSource1.positionCodec().serializePosition(position);

        MergedEventSource eventSource2 = MergedEventSource.streamOrderMergedEventSource(
                clock,
                new NamedReaderWithCodec("a", input1, JavaInMemoryEventStore.CODEC)
        );

        try {
            eventSource2.positionCodec().deserializePosition(serialisedPosition);
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Bad position, containing unexpected keys {\"a\":\"0\",\"b\":\"0\"}"));
        }
    }

    @Test public void
    position_ordering() throws Exception {
        JavaInMemoryEventStore input1 = new JavaInMemoryEventStore(clock);
        JavaInMemoryEventStore input2 = new JavaInMemoryEventStore(clock);
        MergedEventSource eventSource1 = MergedEventSource.streamOrderMergedEventSource(
                clock,
                new NamedReaderWithCodec("a", input1, JavaInMemoryEventStore.CODEC),
                new NamedReaderWithCodec("b", input2, JavaInMemoryEventStore.CODEC)
        );

        Position emptyStorePosition = eventSource1.readAll().emptyStorePosition();
        inputEventArrived(input1, streamId("all", "all"), "CoolenessAdded");
        Position a1 = eventSource1.readAll().readAllForwards().map(ResolvedEvent::position).reduce(emptyStorePosition, (cur, next) -> next);
        clock.bump(Duration.ofMinutes(1));
        inputEventArrived(input2, streamId("all", "all"), "MoreCoolenessAdded");
        Position b1 = eventSource1.readAll().readAllForwards().map(ResolvedEvent::position).reduce(emptyStorePosition, (cur, next) -> next);

        assertThat(eventSource1.positionCodec().comparePositions(emptyStorePosition, emptyStorePosition), equalTo(0));
        assertThat(eventSource1.positionCodec().comparePositions(emptyStorePosition, a1), lessThan(0));
        assertThat(eventSource1.positionCodec().comparePositions(a1, emptyStorePosition), greaterThan(0));
        assertThat(eventSource1.positionCodec().comparePositions(b1, a1), greaterThan(0));
        assertThat(eventSource1.positionCodec().comparePositions(b1, emptyStorePosition), greaterThan(0));
        assertThat(a1.toString(), equalTo("a:1;b:0"));
        assertThat(b1.toString(), equalTo("a:1;b:1"));
    }

    @Test public void
    on_reordering_partial_positions_are_comparable_whilst_merged_position_is_not() {
        JavaInMemoryEventStore input1 = new JavaInMemoryEventStore(clock);
        JavaInMemoryEventStore input2 = new JavaInMemoryEventStore(clock);
        MergedEventSource merged = MergedEventSource.effectiveTimestampMergedEventSource(
                clock,
                new NamedReaderWithCodec("a", input1, JavaInMemoryEventStore.CODEC),
                new NamedReaderWithCodec("b", input2, JavaInMemoryEventStore.CODEC)
        );

        input1.write(streamId("all", "all"), ImmutableList.of(newEvent("", new byte[0], "{\"effective_timestamp\":\"2014-01-23T00:23:54Z\"}".getBytes(UTF_8))));
        input2.write(streamId("all", "all"), ImmutableList.of(newEvent("", new byte[0], "{\"effective_timestamp\":\"2014-01-23T00:23:55Z\"}".getBytes(UTF_8))));
        input1.write(streamId("all", "all"), ImmutableList.of(newEvent("", new byte[0], "{\"effective_timestamp\":\"2014-01-23T00:23:57Z\"}".getBytes(UTF_8))));

        Position originalValidPosition = merged.readAll().readAllForwards().collect(Collectors.toList()).get(2).position();

        input2.write(streamId("all", "all"), ImmutableList.of(newEvent("", new byte[0], "{\"effective_timestamp\":\"2014-01-23T00:23:56Z\"}".getBytes(UTF_8))));

        Position postDelayPosition = merged.readAll().readAllForwards().collect(Collectors.toList()).get(2).position();

        assertThat(merged.positionCodecComparing("a").comparePositions(originalValidPosition, postDelayPosition), is(1));
        assertThat(merged.positionCodecComparing("b").comparePositions(originalValidPosition, postDelayPosition), is(-1));

        thrown.expectMessage("Not comparable: a:2;b:1 <=> a:1;b:2");
        merged.positionCodec().comparePositions(originalValidPosition, postDelayPosition);
    }

    private static void inputEventArrived(EventStreamWriter input, String eventType) {
        inputEventArrived(input, streamId("all", "all"), eventType);
    }

    private static void inputEventArrived(EventStreamWriter input, StreamId streamId, String eventType) {
        inputEventArrived(input, streamId, newEvent(eventType, new byte[0], new byte[0]));
    }

    private static void inputEventArrived(EventStreamWriter input, StreamId streamId, NewEvent event) {
        input.write(streamId, singleton(event));
    }
}