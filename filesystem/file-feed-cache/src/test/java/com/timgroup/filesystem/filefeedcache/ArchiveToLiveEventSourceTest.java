package com.timgroup.filesystem.filefeedcache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.archiver.S3ArchiveKeyFormat;
import com.timgroup.eventstore.memory.InMemoryEventSource;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static com.timgroup.filesystem.filefeedcache.FileFeedCacheEventSourceTest.archivedEvent;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

public final class ArchiveToLiveEventSourceTest {
    private static final StreamId STREAM_ID = StreamId.streamId("category_1", "stream1");
    private static final String EVENT_STORE_ARCHIVE_ID = "anEventStoreId";

    FileFeedCacheEventSourceTest.FakeReadableFeedStorage storage = new FileFeedCacheEventSourceTest.FakeReadableFeedStorage(ImmutableMap.of(
            "anEventStoreId/0002.gz", ImmutableList.of(archivedEvent(1), archivedEvent(2))
    ));
    private final EventSource archive = new FileFeedCacheEventSource(EVENT_STORE_ARCHIVE_ID, storage, new S3ArchiveKeyFormat(EVENT_STORE_ARCHIVE_ID));
    private final EventSource live = new InMemoryEventSource(new JavaInMemoryEventStore(java.time.Clock.systemUTC()));
    private ArchiveToLiveEventSource eventSource;

    @Before
    public void setup() {
        EventStreamWriter liveWriter = live.writeStream();
        liveWriter.write(STREAM_ID, Collections.singleton(newEvent("ArchiveEvent1_inLive")));
        liveWriter.write(STREAM_ID, Collections.singleton(newEvent("ArchiveEvent2_inLive")));
        liveWriter.write(STREAM_ID, Collections.singleton(newEvent("LiveEvent1")));
        liveWriter.write(STREAM_ID, Collections.singleton(newEvent("LiveEvent2")));

        Position maxPositionInArchive = live.readAll().storePositionCodec().deserializePosition("2");
        eventSource = new ArchiveToLiveEventSource(archive, live, maxPositionInArchive);
    }

    @Test public void
    it_reads_all_events_from_archive_and_those_required_from_live_when_querying_entire_event_stream() {
        List<String> readEvents = eventSource.readAll().readAllForwards()
                .map(ResolvedEvent::eventRecord)
                .map(EventRecord::eventType)
                .collect(toList());

        assertThat(readEvents, contains("ArchiveEvent1", "ArchiveEvent2", "LiveEvent1", "LiveEvent2"));
    }

    @Test public void
    generates_transparent_archive_to_live_positions() {
        List<String> readEvents = eventSource.readAll().readAllForwards()
                .map(ResolvedEvent::position)
                .map(p -> eventSource.readAll().storePositionCodec().serializePosition(p))
                .collect(toList());

        assertThat(readEvents, contains("1", "2", "3", "4"));
    }

    @Test public void
    it_returns_only_events_from_live_if_reading_from_after_max_position_in_archive() {
        Position startPositionExclusive = eventSource.readAll().readAllForwards().limit(3).collect(toList()).get(2).position();

        List<String> readEvents = eventSource.readAll().readAllForwards(startPositionExclusive)
                .map(ResolvedEvent::eventRecord)
                .map(EventRecord::eventType)
                .collect(toList());

        assertThat(readEvents, contains("LiveEvent2"));
    }

    private static NewEvent newEvent(String eventType) {
        return NewEvent.newEvent(eventType, eventType.getBytes(UTF_8), new StringBuilder(eventType).reverse().toString().getBytes(UTF_8));
    }
}