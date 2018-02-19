package com.timgroup.eventstore.api.legacy;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public class ReadableLegacyStoreTest {
    @Test
    public void passes_all_events_to_event_handler() throws Exception {
        List<String> received = new ArrayList<>();
        new ReadableLegacyStore(events("red", "orange", "yellow"), TestEventReader::toPosition, TestEventReader::toVersion)
                .streamingFromAll(0).forEachOrdered(e -> received.add(e.version() + ":" + new String(e.eventData().body().data(), UTF_8)));
        assertEquals(Arrays.asList("1:red", "2:orange", "3:yellow"), received);
    }

    @Test
    public void passes_tail_events_to_event_handler() throws Exception {
        List<String> received = new ArrayList<>();
        new ReadableLegacyStore(events("red", "orange", "yellow"), TestEventReader::toPosition, TestEventReader::toVersion)
                .streamingFromAll(1).forEachOrdered(e -> received.add(e.version() + ":" + new String(e.eventData().body().data(), UTF_8)));
        assertEquals(Arrays.asList("2:orange", "3:yellow"), received);
    }

    @Test
    public void provides_all_events_as_event_stream() throws Exception {
        List<String> received = new ArrayList<>();
        new ReadableLegacyStore(events("red", "orange", "yellow"), TestEventReader::toPosition, TestEventReader::toVersion)
                .streamingFromAll(0).forEachOrdered(e -> received.add(e.version() + ":" + new String(e.eventData().body().data(), UTF_8)));
        assertEquals(Arrays.asList("1:red", "2:orange", "3:yellow"), received);
    }

    @Test
    public void provides_tail_events_as_event_stream() throws Exception {
        List<String> received = new ArrayList<>();
        new ReadableLegacyStore(events("red", "orange", "yellow"), TestEventReader::toPosition, TestEventReader::toVersion)
                .streamingFromAll(1).forEachOrdered(e -> received.add(e.version() + ":" + new String(e.eventData().body().data(), UTF_8)));
        assertEquals(Arrays.asList("2:orange", "3:yellow"), received);
    }

    private static EventReader events(String... contents) {
        Instant timestamp = Instant.parse("2016-10-07T01:27:41Z");
        StreamId streamId = StreamId.streamId("test", "0");
        EventRecord[] records = new EventRecord[contents.length];
        byte[] metadata = "{}".getBytes(UTF_8);
        for (int i = 0; i < contents.length; i++) {
            records[i] = EventRecord.eventRecord(timestamp.plus(Duration.ofSeconds(i)), streamId, i,  "Test", contents[i].getBytes(UTF_8), metadata);
        }
        return new TestEventReader(Arrays.asList(records));
    }

    private static final class TestEventReader implements EventReader {
        private final List<ResolvedEvent> events;

        static long toVersion(Position position) {
            return ((Pos) position).index + 1;
        }

        static Position toPosition(long version) {
            return new Pos((int) version - 1);
        }

        TestEventReader(List<EventRecord> events) {
            this.events = new ArrayList<>(events.size());
            int index = 0;
            for (EventRecord event : events) {
                this.events.add(new ResolvedEvent(new Pos(index++), event));
            }
        }

        @Override
        public Stream<ResolvedEvent> readAllForwards() {
            return events.stream();
        }

        @Override
        public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
            return events.subList(((Pos) positionExclusive).index + 1, events.size()).stream();
        }

        @Override
        public Position emptyStorePosition() {
            return toPosition(-1L);
        }

        private static final class Pos implements Position {
            private final int index;

            Pos(int index) {
                this.index = index;
            }

            @Override
            public String toString() {
                return Integer.toString(index);
            }
        }
    }
}
