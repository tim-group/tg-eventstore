package com.timgroup.eventstore.diffing.utils;

import com.google.common.collect.ImmutableList;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.diffing.utils.ForkedStreamsLocator.StreamPair;
import com.timgroup.eventstore.memory.InMemoryEventSource;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import org.junit.Test;

import java.time.Clock;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

public class ForkedStreamsLocatorTest {
    private static final StreamId ORIG_ID = StreamId.streamId("all", "all");
    private static final StreamId FORK_ID = StreamId.streamId("all", "all_1");

    @Test public void
    locates_originalStream_past_lastWrittenEventNumber_and_forkedStream_without_IdemPotentStreamForked_event() throws Throwable {
        EventSource eventSource = new InMemoryEventSource(new JavaInMemoryEventStore(Clock.systemUTC()));

        eventSource.writeStream().write(ORIG_ID, ImmutableList.of(
                event("event1", "event1"),
                event("event2", "event2"),
                event("event3", "event3")
        ));
        eventSource.writeStream().write(FORK_ID, ImmutableList.of(
                event("IdempotentStreamForked", "{\"forkCount\":1,\"oldStream\":{\"category\":\"all\",\"id\":\"all\"},\"lastWrittenEventNumber\":1,\"newStream\":{\"category\":\"all\",\"id\":\"all_1\"}}"),
                event("fork1", "fork1")
        ));
        eventSource.writeStream().write(ORIG_ID, ImmutableList.of(
                event("event4", "event4")
        ));
        eventSource.writeStream().write(FORK_ID, ImmutableList.of(
                event("fork2", "fork2"),
                event("fork3", "fork3")
        ));

        StreamPair<ResolvedEvent> streams = ForkedStreamsLocator.locateForkedStreams(eventSource, eventSource, FORK_ID);
        List<String> origEvents = streams.originalStream.map(e -> e.eventRecord().eventType()).collect(toList());
        List<String> forkEvents = streams.forkedStream.map(e -> e.eventRecord().eventType()).collect(toList());

        assertThat(origEvents, contains("event3", "event4"));
        assertThat(forkEvents, contains("fork1", "fork2", "fork3"));
    }

    private static NewEvent event(String type, String data) {
        return NewEvent.newEvent(type, data.getBytes(UTF_8), new byte[0]);
    }
}
