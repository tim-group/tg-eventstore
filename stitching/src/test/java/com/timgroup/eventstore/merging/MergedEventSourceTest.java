package com.timgroup.eventstore.merging;

import com.timgroup.clocks.testing.ManualClock;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
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

public final class MergedEventSourceTest {

    private final ManualClock clock = new ManualClock(Instant.parse("2009-04-12T22:12:32Z"), ZoneId.of("UTC"));


    @Test public void
    supports_reading_all_forwards_from_a_single_input_stream() throws Exception {
        JavaInMemoryEventStore input = new JavaInMemoryEventStore(clock);

        EventReader outputReader = MergedEventSource.streamOrderMergedEventSource(new NamedReaderWithCodec("a", input, input)).readAll();

        inputEventArrived(input, "CoolenessAdded");
        inputEventArrived(input, "CoolenessChanged");
        inputEventArrived(input, "CoolenessRemoved");

        List<EventRecord> mergedEvents = outputReader.readAllForwards().map(ResolvedEvent::eventRecord).collect(Collectors.toList());

        assertThat(mergedEvents, contains(
                anEventRecord(
                        clock.instant(),
                        streamId("input", "all"),
                        0L,
                        "CoolenessAdded",
                        new byte[0],
                        new byte[0]
                ),
                anEventRecord(
                        clock.instant(),
                        streamId("input", "all"),
                        1L,
                        "CoolenessChanged",
                        new byte[0],
                        new byte[0]
                ),
                anEventRecord(
                        clock.instant(),
                        streamId("input", "all"),
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

        EventReader outputReader = MergedEventSource.streamOrderMergedEventSource(new NamedReaderWithCodec("a", input, input)).readAll();

        inputEventArrived(input, "CoolenessAdded");
        inputEventArrived(input, "CoolenessChanged");

        @SuppressWarnings("OptionalGetWithoutIsPresent") Position startPosition = outputReader.readAllForwards().findFirst().get().position();
        List<EventRecord> mergedEvents = outputReader.readAllForwards(startPosition).map(ResolvedEvent::eventRecord).collect(Collectors.toList());

        assertThat(mergedEvents, contains(
                anEventRecord(
                        clock.instant(),
                        streamId("input", "all"),
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
                new NamedReaderWithCodec("a", input1, input1),
                new NamedReaderWithCodec("b", input2, input2)
        );
        EventReader outputReader = eventSource.readAll();

        inputEventArrived(input1, "CoolenessAdded");
        inputEventArrived(input2, "CoolenessRemoved");
        inputEventArrived(input2, "CoolenessChanged");

        @SuppressWarnings("OptionalGetWithoutIsPresent") Position startPosition = outputReader.readAllForwards().skip(1).findFirst().get().position();
        Position deserialisedStartPosition = eventSource.positionCodec().deserializePosition(eventSource.positionCodec().serializePosition(startPosition));

        inputEventArrived(input1, "CoolenessDestroyed");

        List<EventRecord> mergedEvents = outputReader.readAllForwards(deserialisedStartPosition).map(ResolvedEvent::eventRecord).collect(Collectors.toList());

        assertThat(mergedEvents, contains(
                anEventRecord(
                        clock.instant(),
                        streamId("input", "all"),
                        2L,
                        "CoolenessDestroyed",
                        new byte[0],
                        new byte[0]
                ),
                anEventRecord(
                        clock.instant(),
                        streamId("input", "all"),
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
                new NamedReaderWithCodec("a", input1, input1),
                new NamedReaderWithCodec("b", input2, input2)
        ).readAll();

        inputEventArrived(input1, "CoolenessAdded");
        inputEventArrived(input2, "CoolenessRemoved");
        inputEventArrived(input1, "CoolenessChanged");

        List<EventRecord> mergedEvents = outputReader.readAllForwards().map(ResolvedEvent::eventRecord).collect(Collectors.toList());

        assertThat(mergedEvents, contains(
                anEventRecord(
                        clock.instant(),
                        streamId("input", "all"),
                        0L,
                        "CoolenessAdded",
                        new byte[0],
                        new byte[0]
                ),
                anEventRecord(
                        clock.instant(),
                        streamId("input", "all"),
                        1L,
                        "CoolenessChanged",
                        new byte[0],
                        new byte[0]
                ),
                anEventRecord(
                        clock.instant(),
                        streamId("input", "all"),
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
                new NamedReaderWithCodec("a", input1, input1),
                new NamedReaderWithCodec("b", input2, input2)
        ).readAll();

        inputEventArrived(input1, streamId("baz", "bob"), newEvent("CoolenessAdded",   new byte[0], "{\"effective_timestamp\":\"2014-01-23T00:23:54Z\"}".getBytes(UTF_8)));
        inputEventArrived(input1, streamId("foo", "bar"), newEvent("CoolenessRemoved", new byte[0], "{\"effective_timestamp\":\"2016-01-23T00:23:54Z\"}".getBytes(UTF_8)));
        inputEventArrived(input2, streamId("arg", "erg"), newEvent("CoolenessChanged", new byte[0], "{\"effective_timestamp\":\"2015-01-23T00:23:54Z\"}".getBytes(UTF_8)));

        List<EventRecord> mergedEvents = outputReader.readAllForwards().map(ResolvedEvent::eventRecord).collect(Collectors.toList());

        assertThat(mergedEvents, contains(
                anEventRecord(
                        clock.instant(),
                        streamId("input", "all"),
                        0L,
                        "CoolenessAdded",
                        new byte[0],
                        "{\"effective_timestamp\":\"2014-01-23T00:23:54Z\"}".getBytes(UTF_8)
                ),
                anEventRecord(
                        clock.instant(),
                        streamId("input", "all"),
                        1L,
                        "CoolenessChanged",
                        new byte[0],
                        "{\"effective_timestamp\":\"2015-01-23T00:23:54Z\"}".getBytes(UTF_8)
                ),
                anEventRecord(
                        clock.instant(),
                        streamId("input", "all"),
                        2L,
                        "CoolenessRemoved",
                        new byte[0],
                        "{\"effective_timestamp\":\"2016-01-23T00:23:54Z\"}".getBytes(UTF_8)
                )
        ));
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