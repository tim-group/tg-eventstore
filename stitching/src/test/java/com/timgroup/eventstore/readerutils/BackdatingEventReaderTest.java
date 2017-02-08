package com.timgroup.eventstore.readerutils;

import com.timgroup.clocks.testing.ManualClock;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import org.junit.Test;

import java.time.Instant;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.indicatorinputstreamwriter.ResolvedEventMatcher.aResolvedEvent;
import static java.time.ZoneId.systemDefault;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

public class BackdatingEventReaderTest {
    private ManualClock clock = new ManualClock(Instant.parse("2017-01-02T12:00:00Z"), systemDefault());
    private JavaInMemoryEventStore underlying = new JavaInMemoryEventStore(clock);
    private Instant liveCutoffDate = Instant.parse("2017-01-01T13:00:00Z");
    private BackdatingEventReader reader = new BackdatingEventReader(underlying, liveCutoffDate);
    private StreamId aStream = StreamId.streamId("all", "all");
    private byte[] data = "data".getBytes();
    private byte[] data2 = "data2".getBytes();
    private byte[] metadata = "{\"effective_timestamp\":\"2017-01-01T09:00:00Z\"}".getBytes();
    private byte[] backdatedMetadata = "{\"effective_timestamp\":\"1970-01-01T00:00:00Z\"}".getBytes();

    @Test
    public void it_backdates_events_with_time_before_the_live_cutoff() throws Exception {
        underlying.write(aStream, singletonList(newEvent("AnEvent", data, metadata)));

        assertThat(reader.readAllForwards().collect(toList()),
                contains(aResolvedEvent(position(0), eventRecord(
                        clock.instant(),
                        aStream,
                        0,
                        "AnEvent",
                        data,
                        backdatedMetadata
                ))));
    }

    @Test
    public void it_backdates_events_with_time_before_the_live_cutoff_and_doesnt_touch_other_metadata_fields() throws Exception {
        byte[] metadata = "{\"another_field\":4,\"effective_timestamp\":\"2017-01-01T09:00:00Z\"}".getBytes();
        byte[] backdatedMetadata = "{\"another_field\":4,\"effective_timestamp\":\"1970-01-01T00:00:00Z\"}".getBytes();

        underlying.write(aStream, singletonList(newEvent("AnEvent", data, metadata)));

        assertThat(reader.readAllForwards().collect(toList()),
                contains(aResolvedEvent(position(0), eventRecord(
                        clock.instant(),
                        aStream,
                        0,
                        "AnEvent",
                        data,
                        backdatedMetadata
                ))));
    }

    @Test(expected = IllegalStateException.class)
    public void it_throws_exception_if_no_effective_timestamp_field() throws Exception {
        byte[] metadata = "{\"another_field\":4}".getBytes();

        underlying.write(aStream, singletonList(newEvent("AnEvent", data, metadata)));

        reader.readAllForwards().forEach(e -> {});
    }

    @Test
    public void it_reads_by_position() throws Exception {
        underlying.write(aStream, asList(
                newEvent("AnEvent", data, metadata),
                newEvent("AnEvent", data2, metadata)
        ));

        assertThat(reader.readAllForwards(position(0)).collect(toList()),
                contains(aResolvedEvent(position(1), eventRecord(
                        clock.instant(),
                        aStream,
                        1,
                        "AnEvent",
                        data2,
                        backdatedMetadata
                ))));
    }

    @Test
    public void it_does_not_backdate_events_with_time_at_the_live_cutoff() throws Exception {
        final byte[] metadata = "{\"effective_timestamp\":\"2017-01-01T13:00:00Z\"}".getBytes();

        underlying.write(aStream, singletonList(newEvent("AnEvent", data, metadata)));

        assertThat(reader.readAllForwards().collect(toList()),
                contains(aResolvedEvent(position(0), eventRecord(
                        clock.instant(),
                        aStream,
                        0,
                        "AnEvent",
                        data,
                        metadata
                ))));
    }

    @Test
    public void it_does_not_backdate_events_with_time_after_the_live_cutoff() throws Exception {
        final byte[] metadata = "{\"effective_timestamp\":\"2017-01-01T13:00:00.001Z\"}".getBytes();

        underlying.write(aStream, singletonList(newEvent("AnEvent", data, metadata)));

        assertThat(reader.readAllForwards().collect(toList()),
                contains(aResolvedEvent(position(0), eventRecord(
                        clock.instant(),
                        aStream,
                        0,
                        "AnEvent",
                        data,
                        metadata
                ))));
    }


    @Test
    public void it_reads_backwards() throws Exception {
        underlying.write(aStream, singletonList(newEvent("AnEvent", data, metadata)));

        assertThat(reader.readAllBackwards().collect(toList()),
                contains(aResolvedEvent(position(0), eventRecord(
                        clock.instant(),
                        aStream,
                        0,
                        "AnEvent",
                        data,
                        backdatedMetadata
                ))));
    }

    @Test
    public void it_reads_backwards_by_position() throws Exception {
        underlying.write(aStream, asList(
                newEvent("AnEvent", data, metadata),
                newEvent("AnEvent", data2, metadata)
        ));

        assertThat(reader.readAllBackwards(position(1)).collect(toList()),
                contains(aResolvedEvent(position(0), eventRecord(
                        clock.instant(),
                        aStream,
                        0,
                        "AnEvent",
                        data,
                        backdatedMetadata
                ))));
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    private Position position(int position) {
        return underlying.readAllForwards().skip(position).findFirst().get().position();
    }
}