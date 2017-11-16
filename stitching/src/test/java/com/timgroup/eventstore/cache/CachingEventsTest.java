package com.timgroup.eventstore.cache;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static com.timgroup.eventstore.memory.JavaInMemoryEventStore.CODEC;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class CachingEventsTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    Path cacheDirectory;

    CacheEventReader cacheEventReader;

    JavaInMemoryEventStore underlyingEventStore = new JavaInMemoryEventStore(Clock.systemUTC());

    private final StreamId stream_1 = streamId(randomCategory(), "1");
    private final NewEvent event_1 = anEvent();

    @Before public void init() throws IOException {
        temporaryFolder.create();
        cacheDirectory = temporaryFolder.getRoot().toPath();
        cacheEventReader = new CacheEventReader(underlyingEventStore, CODEC, cacheDirectory);
    }

    private List<ResolvedEvent> readAllToList(EventReader eventReader) {
        return eventReader.readAllForwards().collect(toList());
    }

    private void saveAllToCache() throws Exception {
        try (OutputStream outputStream = new GZIPOutputStream(new FileOutputStream(cacheDirectory.resolve("cache.gz").toFile()));
             CacheEventWriter cacheEventWriter = new CacheEventWriter(outputStream, CODEC)){
            underlyingEventStore.readAllForwards().forEach(cacheEventWriter::write);
        }
    }

    @Test
    public void
    givenNoDataInUnderlying_returnsNothing() {
        assertThat(readAllToList(cacheEventReader), is(empty()));
    }

    @Test
    public void
    givenAnEventDataInUnderlying_returnsThatEvent() throws Exception {
        underlyingEventStore.write(stream_1, singletonList(event_1));
        saveAllToCache();

        assertThat(readAllToList(cacheEventReader), hasSize(1));
        assertThat(readAllToList(cacheEventReader), equalTo(readAllToList(underlyingEventStore)));
    }

    @Test
    public void
    givenCachePopulated_readsFromTheCache() throws Exception {
        underlyingEventStore.write(stream_1, singletonList(event_1));
        saveAllToCache();

        CacheEventReader newCacheEventReader = new CacheEventReader(new JavaInMemoryEventStore(Clock.systemUTC()),  CODEC, cacheDirectory);
        assertThat(readAllToList(newCacheEventReader), hasSize(1));
        assertThat(readAllToList(newCacheEventReader), equalTo(readAllToList(underlyingEventStore)));
    }

    private List<ResolvedEvent> readAllFromEmpty(EventReader eventReader) {
        return eventReader.readAllForwards(underlyingEventStore.emptyStorePosition()).collect(toList());
    }

    @Test
    public void
    givenReadingFromEmptyStorePosition_readsFromTheCache() throws Exception {
        underlyingEventStore.write(stream_1, singletonList(event_1));
        saveAllToCache();

        assertThat(readAllFromEmpty(cacheEventReader), hasSize(1));

        CacheEventReader newCacheEventReader = new CacheEventReader(new JavaInMemoryEventStore(Clock.systemUTC()),  CODEC, cacheDirectory);
        assertThat(readAllFromEmpty(newCacheEventReader), hasSize(1));
        assertThat(readAllFromEmpty(newCacheEventReader), equalTo(readAllToList(underlyingEventStore)));
    }

    private static NewEvent anEvent() {
        return newEvent(randomUUID().toString(), randomData(), randomData());
    }

    private static String randomCategory() {
        return "stream_" + randomUUID().toString().replace("-", "");
    }

    private static byte[] randomData() {
        return ("{\n  \"value\": \"" + randomUUID() + "\"\n}").getBytes(UTF_8);
    }
}
