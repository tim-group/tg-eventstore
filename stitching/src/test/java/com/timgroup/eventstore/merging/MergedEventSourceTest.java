package com.timgroup.eventstore.merging;

import com.timgroup.clocks.testing.ManualClock;
import com.timgroup.eventstore.api.*;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static com.timgroup.indicatorinputstreamwriter.EventRecordMatcher.anEventRecord;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singleton;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

public final class MergedEventSourceTest {

    private final ManualClock clock = new ManualClock(Instant.parse("2009-04-12T22:12:32Z"), ZoneId.of("UTC"));

    @Test public void
    supports_reading_all_forwards_from_a_single_input_stream() throws Exception {
        JavaInMemoryEventStore input = new JavaInMemoryEventStore(clock);

        EventReader outputReader = MergedEventSource.streamOrderMergedEventSource(clock, new NamedReaderWithCodec("a", input, input)).readAll();

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
    supports_reading_from_given_position_from_a_single_input_stream() throws Exception {
        JavaInMemoryEventStore input = new JavaInMemoryEventStore(clock);

        EventReader outputReader = MergedEventSource.streamOrderMergedEventSource(clock, new NamedReaderWithCodec("a", input, input)).readAll();

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
        MergedEventSource<Integer> eventSource = MergedEventSource.streamOrderMergedEventSource(
                clock,
                new NamedReaderWithCodec("a", input1, input1),
                new NamedReaderWithCodec("b", input2, input2)
        );
        EventReader outputReader = eventSource.readAll();

        Instant instant1 = clock.instant();
        inputEventArrived(input1, "CoolenessAdded");
        inputEventArrived(input2, "CoolenessRemoved");
        inputEventArrived(input2, "CoolenessChanged");

        clock.advanceTo(instant1.plusSeconds(1));
        @SuppressWarnings("OptionalGetWithoutIsPresent") Position startPosition = outputReader.readAllForwards().skip(1).findFirst().get().position();
        Position deserialisedStartPosition = eventSource.positionCodec().deserializePosition(eventSource.positionCodec().serializePosition(startPosition));

        Instant instant2 = clock.instant();
        inputEventArrived(input1, "CoolenessDestroyed");

        clock.advanceTo(instant2.plusSeconds(1));
        List<EventRecord> mergedEvents = outputReader.readAllForwards(deserialisedStartPosition).map(ResolvedEvent::eventRecord).collect(Collectors.toList());

        assertThat(mergedEvents, contains(
                anEventRecord(
                        instant2,
                        streamId("all", "all"),
                        2L,
                        "CoolenessDestroyed",
                        new byte[0],
                        new byte[0]
                ),
                anEventRecord(
                        instant1,
                        streamId("all", "all"),
                        3L,
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
                new NamedReaderWithCodec("a", input1, input1),
                new NamedReaderWithCodec("b", input2, input2)
        ).readAll();

        Instant instant = clock.instant();
        inputEventArrived(input1, "CoolenessAdded");
        inputEventArrived(input2, "CoolenessRemoved");
        inputEventArrived(input1, "CoolenessChanged");

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
    supports_reading_all_forwards_from_multiple_input_streams_merging_by_effective_timestamp() throws Exception {
        JavaInMemoryEventStore input1 = new JavaInMemoryEventStore(clock);
        JavaInMemoryEventStore input2 = new JavaInMemoryEventStore(clock);

        EventReader outputReader = MergedEventSource.effectiveTimestampMergedEventSource(
                clock,
                Duration.ZERO,
                streamId("input", "all"),
                new NamedReaderWithCodec("a", input1, input1),
                new NamedReaderWithCodec("b", input2, input2)
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
                        streamId("input", "all"),
                        0L,
                        "CoolenessAdded",
                        new byte[0],
                        "{\"effective_timestamp\":\"2014-01-23T00:23:54Z\"}".getBytes(UTF_8)
                ),
                anEventRecord(
                        instant,
                        streamId("input", "all"),
                        1L,
                        "CoolenessChanged",
                        new byte[0],
                        "{\"effective_timestamp\":\"2015-01-23T00:23:54Z\"}".getBytes(UTF_8)
                ),
                anEventRecord(
                        instant,
                        streamId("input", "all"),
                        2L,
                        "CoolenessRemoved",
                        new byte[0],
                        "{\"effective_timestamp\":\"2016-01-23T00:23:54Z\"}".getBytes(UTF_8)
                )
        ));
    }

    @Test public void
    blows_up_when_merging_by_effective_timestamp_on_encountering_an_event_without_an_effective_timestamp() throws Exception {
        JavaInMemoryEventStore input1 = new JavaInMemoryEventStore(clock);

        EventReader outputReader = MergedEventSource.effectiveTimestampMergedEventSource(clock, new NamedReaderWithCodec("a", input1, input1)).readAll();

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