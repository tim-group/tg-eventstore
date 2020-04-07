package com.timgroup.eventstore.mysql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.tucker.info.Component;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static com.timgroup.eventstore.mysql.FileFeedCacheEventSourceTest.archivedEvent;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

public final class ArchiveToLiveEventSourceTest {
    private static final String EVENT_STORE_ARCHIVE_ID = "anEventStoreId";

    FileFeedCacheEventSourceTest.FakeReadableFeedStorage storage = new FileFeedCacheEventSourceTest.FakeReadableFeedStorage(ImmutableMap.of(
            "anEventStoreId/0002.gz", ImmutableList.of(
                    archivedEvent(1, "A"),
                    archivedEvent(2, "B")
            )
    ));
    private final EventSource archive = new FileFeedCacheEventSource(EVENT_STORE_ARCHIVE_ID, storage);
    private final EventSource live = new FakeMysqlEventSource();
    private ArchiveToLiveEventSource eventSource;

    @Before
    public void setup() {
        EventStreamWriter liveWriter = live.writeStream();
        liveWriter.write(stream("A"), Collections.singleton(newEvent("ArchiveEvent1_inLive")));
        liveWriter.write(stream("B"), Collections.singleton(newEvent("ArchiveEvent2_inLive")));
        liveWriter.write(stream("A"), Collections.singleton(newEvent("LiveEvent1")));
        liveWriter.write(stream("B"), Collections.singleton(newEvent("LiveEvent2")));

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
    it_reads_matching_events_from_archive_and_those_required_from_live_when_querying_a_category() {
        List<String> readEvents = eventSource.readCategory().readCategoryForwards("B")
                .map(ResolvedEvent::eventRecord)
                .map(EventRecord::eventType)
                .collect(toList());

        assertThat(readEvents, contains("ArchiveEvent2", "LiveEvent2"));
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

    private static StreamId stream(String id) {
        return StreamId.streamId(id, id);
    }

    private static final class FakeMysqlEventSource implements EventSource, EventReader, EventCategoryReader, EventStreamWriter {
        private final List<ResolvedEvent> events = new ArrayList<>();

        @Nonnull @Override public EventReader readAll() { return this; }
        @Nonnull @Override public EventStreamWriter writeStream() { return this; }
        @Nonnull @Override public EventCategoryReader readCategory() { return this; }
        @Nonnull @Override public EventStreamReader readStream() { throw new UnsupportedOperationException(); }

        @Nonnull @Override public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
            BasicMysqlEventStorePosition mysqlPositionExclusive = (BasicMysqlEventStorePosition) positionExclusive;
            return events.stream()
                    .filter(event -> mysqlPositionExclusive.compareTo((BasicMysqlEventStorePosition)event.position()) < 0);
        }

        @Nonnull @Override public Stream<ResolvedEvent> readCategoryForwards(String category, Position positionExclusive) {
            return readAllForwards(positionExclusive)
                    .filter(event -> category.equals(event.eventRecord().streamId().category()));
        }

        @Override public void write(StreamId streamId, Collection<NewEvent> newEvents) {
            long nextPosition = events.size() + 1;
            for (NewEvent newEvent : newEvents) {
                events.add(new ResolvedEvent(
                        new BasicMysqlEventStorePosition(nextPosition),
                        newEvent.toEventRecord(Instant.now(), streamId, nextPosition
                )));
                nextPosition++;
            }
        }

        @Override public void write(StreamId streamId, Collection<NewEvent> events, long expectedVersion) {
            write(streamId, events);
        }

        @Nonnull @Override public Collection<Component> monitoring() { return Collections.emptyList(); }


        @Nonnull @Override public Position emptyStorePosition() { return BasicMysqlEventStorePosition.EMPTY_STORE_POSITION; }
        @Nonnull @Override public PositionCodec storePositionCodec() { return BasicMysqlEventStorePosition.CODEC; }
        @Nonnull @Override public Position emptyCategoryPosition(String category) { return BasicMysqlEventStorePosition.EMPTY_STORE_POSITION; }
        @Nonnull @Override public PositionCodec categoryPositionCodec(String category) { return BasicMysqlEventStorePosition.CODEC; }
    }
}