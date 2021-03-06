package com.timgroup.eventstore.mysql;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.timgroup.eventstore.archiver.EventStoreArchiverProtos;
import com.timgroup.eventstore.archiver.EventStoreArchiverProtos.Event;
import com.timgroup.filefeed.reading.ReadableFeedStorage;
import com.timgroup.filefeed.reading.StorageLocation;
import org.joda.time.Instant;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import static com.timgroup.eventstore.mysql.BasicMysqlEventStorePosition.CODEC;
import static com.timgroup.eventstore.mysql.BasicMysqlEventStorePosition.EMPTY_STORE_POSITION;
import static com.timgroup.filefeed.reading.StorageLocation.TimGroupEventStoreFeedStore;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public final class FileFeedCacheEventSourceTest {
    private static final String EVENT_STORE_ID = "anEventStoreId";

    @Test public void
    returns_no_events_for_empty_feed_storage() {
        ReadableFeedStorage emptyStorage = new FakeReadableFeedStorage(ImmutableMap.of());
        FileFeedCacheEventSource reader = new FileFeedCacheEventSource(EVENT_STORE_ID, emptyStorage, EMPTY_STORE_POSITION);

        assertThat(reader.readAllForwards(reader.emptyStorePosition()).collect(toList()), is(empty()));
        assertThat(reader.readLastEvent(), is(Optional.empty()));
    }

    @Test public void
    returns_events_after_given_position() {
        ReadableFeedStorage storage = new FakeReadableFeedStorage(ImmutableMap.of(
                EVENT_STORE_ID + "/0001.gz", ImmutableList.of(archivedEvent(1)),
                EVENT_STORE_ID + "/0003.gz", ImmutableList.of(archivedEvent(2), archivedEvent(3))
        ));
        FileFeedCacheEventSource reader = new FileFeedCacheEventSource(EVENT_STORE_ID, storage, CODEC.deserializePosition("3"));

        List<String> returnedEvents = reader.readAllForwards(reader.storePositionCodec().deserializePosition("1"))
                .map(e -> e.eventRecord().eventType()).collect(toList());

        assertThat(returnedEvents, contains("ArchiveEvent2", "ArchiveEvent3"));
    }


    @Test public void
    returns_last_event() {
        ReadableFeedStorage storage = new FakeReadableFeedStorage(ImmutableMap.of(
                EVENT_STORE_ID + "/0001.gz", ImmutableList.of(archivedEvent(1)),
                EVENT_STORE_ID + "/0003.gz", ImmutableList.of(archivedEvent(2), archivedEvent(3))
        ));
        FileFeedCacheEventSource reader = new FileFeedCacheEventSource(EVENT_STORE_ID, storage, CODEC.deserializePosition("3"));

        assertThat(reader.readLastEvent().map(e -> e.eventRecord().eventType()), is(Optional.of("ArchiveEvent3")));
    }

    @Test public void
    only_accesses_required_files() {
        FakeReadableFeedStorage storage = new FakeReadableFeedStorage(ImmutableMap.of(
                EVENT_STORE_ID + "/0002.gz", ImmutableList.of(archivedEvent(1), archivedEvent(2)),
                EVENT_STORE_ID + "/0004.gz", ImmutableList.of(archivedEvent(3), archivedEvent(4))
        ));
        FileFeedCacheEventSource reader = new FileFeedCacheEventSource(EVENT_STORE_ID, storage, CODEC.deserializePosition("4"));

        reader.readAllForwards(reader.storePositionCodec().deserializePosition("2")).forEach(e -> {});

        assertThat(storage.accessedFiles, contains(EVENT_STORE_ID + "/0004.gz"));
    }

    @Test public void
    reads_by_given_category() {
        ReadableFeedStorage storage = new FakeReadableFeedStorage(ImmutableMap.of(EVENT_STORE_ID + "/0004.gz",
                ImmutableList.of(
                        archivedEvent(1, "interestingCategory"),
                        archivedEvent(2, "interestingCategory"),
                        archivedEvent(3, "otherCategory"),
                        archivedEvent(4, "interestingCategory")
                )
        ));
        FileFeedCacheEventSource reader = new FileFeedCacheEventSource(EVENT_STORE_ID, storage, CODEC.deserializePosition("4"));

        List<String> returnedEvents = reader.readCategory()
                .readCategoryForwards("interestingCategory", reader.storePositionCodec().deserializePosition("1"))
                .map(e -> e.eventRecord().eventType()).collect(toList());

        assertThat(returnedEvents, contains("ArchiveEvent2", "ArchiveEvent4"));
    }

    @Test public void
    reads_by_given_categories() {
        ReadableFeedStorage storage = new FakeReadableFeedStorage(ImmutableMap.of(EVENT_STORE_ID + "/0004.gz",
                ImmutableList.of(
                        archivedEvent(1, "interestingCategory1"),
                        archivedEvent(2, "otherCategory"),
                        archivedEvent(3, "interestingCategory2"),
                        archivedEvent(4, "interestingCategory1")
                )
        ));
        FileFeedCacheEventSource reader = new FileFeedCacheEventSource(EVENT_STORE_ID, storage, CODEC.deserializePosition("4"));

        List<String> returnedEvents = reader.readCategory()
                .readCategoriesForwards(
                        Arrays.asList("interestingCategory1", "interestingCategory2"),
                        reader.storePositionCodec().deserializePosition("1"))
                .map(e -> e.eventRecord().eventType()).collect(toList());

        assertThat(returnedEvents, contains("ArchiveEvent3", "ArchiveEvent4"));

    }

    @Test public void
    returns_events_only_upto_the_max_position_retrieved() {
        FakeReadableFeedStorage storage = new FakeReadableFeedStorage(ImmutableMap.of(
                EVENT_STORE_ID + "/0002.gz", ImmutableList.of(archivedEvent(1), archivedEvent(2)),
                EVENT_STORE_ID + "/0004.gz", ImmutableList.of(archivedEvent(3), archivedEvent(4)),
                EVENT_STORE_ID + "/0005.gz", ImmutableList.of(archivedEvent(5), archivedEvent(6))
        ));
        FileFeedCacheEventSource reader = new FileFeedCacheEventSource(EVENT_STORE_ID, storage, CODEC.deserializePosition("4"));

        List<String> returnedEvents = reader.readAllForwards(reader.storePositionCodec().deserializePosition("1"))
                .map(e -> e.eventRecord().eventType()).collect(toList());

        assertThat(returnedEvents, contains("ArchiveEvent2", "ArchiveEvent3", "ArchiveEvent4"));
    }

    static Event archivedEvent(long position) {
        return archivedEvent(position, "aStreamCategory");
    }

    static Event archivedEvent(long position, String streamCategory) {
        return Event.newBuilder()
                .setPosition(position).setEventNumber(position)
                .setEventType("ArchiveEvent" + position)
                .setTimestamp(EventStoreArchiverProtos.Timestamp.newBuilder().setSeconds(position).setNanos(0).build())
                .setStreamCategory(streamCategory).setStreamId("aStreamId")
                .setData(ByteString.EMPTY).setMetadata(ByteString.EMPTY)
                .build();
    }

    static final class FakeReadableFeedStorage implements ReadableFeedStorage {
        private final Map<String, List<Event>> eventsByFile;
        private final Map<String, Instant> arrivalTimeByFile = new HashMap<>();
        private Set<String> accessedFiles = new HashSet<>();

        FakeReadableFeedStorage(Map<String, List<Event>> eventsByFile) {
            this.eventsByFile = eventsByFile;
        }

        public void setArrivalTime(String fileName, Instant arrivalTime) {
            arrivalTimeByFile.put(fileName, arrivalTime);
        }

        @Override public Optional<Instant> getArrivalTime(StorageLocation storageLocation, String fileName) {
            return storageLocation == TimGroupEventStoreFeedStore && eventsByFile.containsKey(fileName)
                    ? Optional.of(arrivalTimeByFile.getOrDefault(fileName, Instant.EPOCH))
                    : Optional.empty();
        }

        @Override public InputStream get(StorageLocation storageLocation, String fileName) {
            Preconditions.checkArgument(storageLocation == TimGroupEventStoreFeedStore);
            Preconditions.checkArgument(eventsByFile.containsKey(fileName));
            accessedFiles.add(fileName);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
                for (Event event : eventsByFile.get(fileName)) {
                    gzip.write(toBytesPrefixedWithLength(event));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return new ByteArrayInputStream(out.toByteArray());
        }

        @Override public List<String> list(StorageLocation storageLocation, String feedName) {
            return storageLocation == TimGroupEventStoreFeedStore && feedName.equals(EVENT_STORE_ID + "/")
                    ? ImmutableList.sortedCopyOf(eventsByFile.keySet())
                    : Collections.emptyList();
        }

        private byte[] toBytesPrefixedWithLength(Message message) {
            byte[] srcArray = message.toByteArray();
            ByteBuffer buffer= ByteBuffer.allocate(srcArray.length +4).order(LITTLE_ENDIAN);
            buffer.putInt(srcArray.length);
            buffer.put(srcArray);
            return buffer.array();
        }
    }
}