package com.timgroup.filesystem.filefeedcache;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.timgroup.eventstore.archiver.EventStoreArchiverProtos;
import com.timgroup.eventstore.archiver.EventStoreArchiverProtos.Event;
import com.timgroup.eventstore.archiver.S3ArchiveKeyFormat;
import com.timgroup.filefeed.reading.ReadableFeedStorage;
import com.timgroup.filefeed.reading.StorageLocation;
import org.joda.time.Instant;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

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
        FileFeedCacheEventSource reader = new FileFeedCacheEventSource(emptyStorage, new S3ArchiveKeyFormat(EVENT_STORE_ID));

        assertThat(reader.readAllForwards(reader.emptyStorePosition()).collect(toList()), is(empty()));
        assertThat(reader.readLastEvent(), is(Optional.empty()));
    }

    @Test public void
    returns_events_after_given_position() {
        ReadableFeedStorage storage = new FakeReadableFeedStorage(ImmutableMap.of(
                EVENT_STORE_ID + "/0001.gz", ImmutableList.of(event(1)),
                EVENT_STORE_ID + "/0003.gz", ImmutableList.of(event(2), event(3))
        ));
        FileFeedCacheEventSource reader = new FileFeedCacheEventSource(storage, new S3ArchiveKeyFormat(EVENT_STORE_ID));

        List<String> returnedEvents = reader.readAllForwards(reader.storePositionCodec().deserializePosition("1"))
                .map(e -> e.eventRecord().eventType()).collect(toList());

        assertThat(returnedEvents, contains("event2", "event3"));
    }

    @Test public void
    returns_last_event() {
        ReadableFeedStorage storage = new FakeReadableFeedStorage(ImmutableMap.of(
                EVENT_STORE_ID + "/0001.gz", ImmutableList.of(event(1)),
                EVENT_STORE_ID + "/0003.gz", ImmutableList.of(event(2), event(3))
        ));
        FileFeedCacheEventSource reader = new FileFeedCacheEventSource(storage, new S3ArchiveKeyFormat(EVENT_STORE_ID));

        assertThat(reader.readLastEvent().map(e -> e.eventRecord().eventType()), is(Optional.of("event3")));
    }

    @Test public void
    only_accesses_required_files() {
        FakeReadableFeedStorage storage = new FakeReadableFeedStorage(ImmutableMap.of(
                EVENT_STORE_ID + "/0002.gz", ImmutableList.of(event(1), event(2)),
                EVENT_STORE_ID + "/0004.gz", ImmutableList.of(event(3), event(4))
        ));
        FileFeedCacheEventSource reader = new FileFeedCacheEventSource(storage, new S3ArchiveKeyFormat(EVENT_STORE_ID));

        reader.readAllForwards(reader.storePositionCodec().deserializePosition("2")).forEach(e -> {});

        assertThat(storage.accessedFiles, contains(EVENT_STORE_ID + "/0004.gz"));
    }

    private static Event event(long position) {
        return Event.newBuilder()
                .setPosition(position).setEventNumber(position)
                .setEventType("event" + position)
                .setTimestamp(EventStoreArchiverProtos.Timestamp.newBuilder().setSeconds(position).setNanos(0).build())
                .setStreamCategory("aStreamCategory").setStreamId("aStreamId")
                .setData(ByteString.EMPTY).setMetadata(ByteString.EMPTY)
                .build();
    }

    private static final class FakeReadableFeedStorage implements ReadableFeedStorage {
        private final Map<String, List<Event>> eventsByFile;
        private Set<String> accessedFiles = new HashSet<>();

        private FakeReadableFeedStorage(Map<String, List<Event>> eventsByFile) {
            this.eventsByFile = eventsByFile;
        }

        @Override public Optional<Instant> getArrivalTime(StorageLocation storageLocation, String fileName) {
            return storageLocation == TimGroupEventStoreFeedStore && eventsByFile.containsKey(fileName)
                    ? Optional.of(Instant.EPOCH)
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