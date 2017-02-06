package com.timgroup.eventstore.merging;

import com.timgroup.clocks.testing.ManualClock;
import com.timgroup.eventstore.api.EventRecord;
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
import static com.timgroup.indicatorinputstreamwriter.EventRecordMatcher.anEventRecord;
import static java.util.Collections.singleton;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

public class MergedEventReaderTest {

    private final ManualClock clock = new ManualClock(Instant.parse("2009-04-12T22:12:32Z"), ZoneId.of("UTC"));

    @Test public void
    supports_reading_all_forwards_from_a_single_input_stream() throws Exception {
        JavaInMemoryEventStore input = new JavaInMemoryEventStore(clock);

        MergedEventReader outputReader = new MergedEventReader(input);

        inputEventArrived(input, "CoolenessAdded");
        inputEventArrived(input, "CoolenessChanged");
        inputEventArrived(input, "CoolenessRemoved");

        List<EventRecord> mergedEvents = outputReader.readAllForwards().map(ResolvedEvent::eventRecord).collect(Collectors.toList());

        assertThat(mergedEvents, contains(
                anEventRecord(
                        clock.instant(),
                        StreamId.streamId("input", "all"),
                        0L,
                        "CoolenessAdded",
                        new byte[0],
                        new byte[0]
                ),
                anEventRecord(
                        clock.instant(),
                        StreamId.streamId("input", "all"),
                        1L,
                        "CoolenessChanged",
                        new byte[0],
                        new byte[0]
                ),
                anEventRecord(
                        clock.instant(),
                        StreamId.streamId("input", "all"),
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

        MergedEventReader outputReader = new MergedEventReader(input);

        inputEventArrived(input, "CoolenessAdded");
        inputEventArrived(input, "CoolenessChanged");

        @SuppressWarnings("OptionalGetWithoutIsPresent") Position startPosition = outputReader.readAllForwards().findFirst().get().position();
        List<EventRecord> mergedEvents = outputReader.readAllForwards(startPosition).map(ResolvedEvent::eventRecord).collect(Collectors.toList());

        assertThat(mergedEvents, contains(
                anEventRecord(
                        clock.instant(),
                        StreamId.streamId("input", "all"),
                        1L,
                        "CoolenessChanged",
                        new byte[0],
                        new byte[0]
                )
        ));
    }


    private static void inputEventArrived(JavaInMemoryEventStore input, String eventType) {
        inputEventArrived(input, StreamId.streamId("all", "all"), eventType);
    }

    private static void inputEventArrived(JavaInMemoryEventStore input, StreamId streamId, String eventType) {
        inputEventArrived(input, streamId, newEvent(eventType, new byte[0], new byte[0]));
    }

    private static void inputEventArrived(JavaInMemoryEventStore input, StreamId streamId, NewEvent event) {
        input.write(streamId, singleton(event));
    }

}