package com.timgroup.eventstore.readerutils;

import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;

import com.timgroup.clocks.testing.ManualClock;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import org.junit.Test;

import static com.timgroup.eventstore.api.EventRecordMatcher.anEventRecord;
import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static java.util.Collections.singleton;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public final class RekeyingEventReaderTest {

    private final ManualClock clock = new ManualClock(Instant.parse("2009-04-12T22:12:32Z"), ZoneId.of("UTC"));

    @Test
    public void
    supports_reading_from_all_forwards_from_multiple_input_streams() throws Exception {
        JavaInMemoryEventStore input1 = new JavaInMemoryEventStore(clock);

        RekeyingEventReader reader = RekeyingEventReader.rekeying(input1, JavaInMemoryEventStore.CODEC, streamId("food", "all"));

        Instant instant = clock.instant();
        inputEventArrived(input1, streamId("cheese", "brie"), "CoolenessAdded");
        inputEventArrived(input1, streamId("cheese", "cheddar"), "CoolenessRemoved");
        inputEventArrived(input1, streamId("meat", "ham"), "CoolenessChanged");
        inputEventArrived(input1, streamId("cheese", "stilton"), "CoolenessExceeded");

        List<EventRecord> mergedEvents = reader.readAllForwards().map(ResolvedEvent::eventRecord).collect(Collectors.toList());
        assertThat(mergedEvents, contains(
                anEventRecord(
                        instant,
                        streamId("food", "all"),
                        0L,
                        "CoolenessAdded",
                        new byte[0],
                        new byte[0]
                ),
                anEventRecord(
                        instant,
                        streamId("food", "all"),
                        1L,
                        "CoolenessRemoved",
                        new byte[0],
                        new byte[0]
                ),
                anEventRecord(
                        instant,
                        streamId("food", "all"),
                        2L,
                        "CoolenessChanged",
                        new byte[0],
                        new byte[0]
                )
                ,
                anEventRecord(
                        instant,
                        streamId("food", "all"),
                        3L,
                        "CoolenessExceeded",
                        new byte[0],
                        new byte[0]
                )
        ));
    }

    @Test
    public void
    supports_reading_from_a_given_position_from_multiple_input_streams() throws Exception {
        JavaInMemoryEventStore input1 = new JavaInMemoryEventStore(clock);

        RekeyingEventReader reader = RekeyingEventReader.rekeying(input1, JavaInMemoryEventStore.CODEC, streamId("food", "all"));

        Instant instant = clock.instant();
        inputEventArrived(input1, streamId("cheese", "brie"), "CoolenessAdded");
        inputEventArrived(input1, streamId("cheese", "cheddar"), "CoolenessRemoved");
        inputEventArrived(input1, streamId("meat", "ham"), "CoolenessChanged");
        inputEventArrived(input1, streamId("cheese", "stilton"), "CoolenessExceeded");

        @SuppressWarnings("OptionalGetWithoutIsPresent")
        Position position = reader.readAllForwards().skip(1).findFirst().get().position();

        String serializedPosition = reader.positionCodec().serializePosition(position);
        assertThat(serializedPosition, is(equalTo("1:2")));

        Position deserializedPosition = reader.positionCodec().deserializePosition(serializedPosition);
        assertThat(deserializedPosition, is(equalTo(position)));

        List<EventRecord> mergedEvents = reader.readAllForwards(deserializedPosition).map(ResolvedEvent::eventRecord).collect(Collectors.toList());
        assertThat(mergedEvents, contains(
                anEventRecord(
                        instant,
                        streamId("food", "all"),
                        2L,
                        "CoolenessChanged",
                        new byte[0],
                        new byte[0]
                )
                ,
                anEventRecord(
                        instant,
                        streamId("food", "all"),
                        3L,
                        "CoolenessExceeded",
                        new byte[0],
                        new byte[0]
                )
        ));
    }

    private static void inputEventArrived(EventStreamWriter input, StreamId streamId, String eventType) {
        inputEventArrived(input, streamId, newEvent(eventType, new byte[0], new byte[0]));
    }

    private static void inputEventArrived(EventStreamWriter input, StreamId streamId, NewEvent event) {
        input.write(streamId, singleton(event));
    }

}