package com.timgroup.eventstore.cache;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPOutputStream;

import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static com.timgroup.eventstore.memory.JavaInMemoryEventStore.CODEC;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class CachingEventsTest {
    private static final Clock CLOCK = Clock.tick(Clock.systemUTC(), Duration.ofMillis(1));

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    Path cacheDirectory;

    CacheEventReader cacheEventReader;

    JavaInMemoryEventStore underlyingEventStore = new JavaInMemoryEventStore(CLOCK);

    private final StreamId stream_1 = streamId(randomCategory(), "1");
    private final NewEvent event_1 = anEvent();

    @Before
    public void init() throws IOException {
        temporaryFolder.create();
        cacheDirectory = temporaryFolder.getRoot().toPath();
        cacheEventReader = new CacheEventReader(underlyingEventStore, CODEC, cacheDirectory, "cache");
    }

    private List<ResolvedEvent> readAllToList(EventReader eventReader) {
        return eventReader.readAllForwards().collect(toList());
    }

    private Position saveAllToCache(File cacheFile) throws Exception {
        return saveAllToCache(cacheFile, underlyingEventStore.emptyStorePosition());
    }

    private Position saveAllToCache(File cacheFile, Position position) throws Exception {
        AtomicReference<Position> lastPosition = new AtomicReference<>();
        try (OutputStream outputStream = new GZIPOutputStream(new FileOutputStream(cacheFile));
             CacheEventWriter cacheEventWriter = new CacheEventWriter(outputStream, CODEC)) {
            underlyingEventStore.readAllForwards(position).forEachOrdered(resolvedEvent -> {
                cacheEventWriter.write(resolvedEvent);
                lastPosition.set(resolvedEvent.position());
            });
        }
        return lastPosition.get();
    }

    private File getCacheFile(String cacheFileName) {
        return cacheDirectory.resolve(cacheFileName).toFile();
    }

    private DataInputStream gzipCache() {
        return new CacheInputStreamSupplier(getCacheFile("cache.gz"), true).get();
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
        saveAllToCache(getCacheFile("cache.gz"));

        assertThat(readAllToList(cacheEventReader), hasSize(1));
        assertThat(readAllToList(cacheEventReader), equalTo(readAllToList(underlyingEventStore)));
    }

    @Test
    public void
    givenEmptyCache_returnsEmptyLastPosition() throws Exception {
        saveAllToCache(getCacheFile("cache.gz"));
        assertThat(CacheEventReader.findLastPosition(gzipCache(), CODEC), is(Optional.empty()));
    }

    @Test(expected = CacheNotFoundException.class)
    public void
    givenNonExistentCache_throwsNonExistentCacheException() throws Exception {
        CacheEventReader.findLastPosition(new CacheInputStreamSupplier(getCacheFile("NON_EXISTENT_FOO")).get(), CODEC);
    }

    @Test
    public void
    givenAnEventsWrittenToCache_returnsTheLastPosition() throws Exception {
        underlyingEventStore.write(stream_1, singletonList(event_1));
        underlyingEventStore.write(stream_1, singletonList(event_1));
        saveAllToCache(getCacheFile("cache.gz"));

        Position currentPosition = underlyingEventStore.readAllBackwards().findFirst().get().position();
        assertThat(CacheEventReader.findLastPosition(gzipCache(), CODEC).get(), is(currentPosition));
    }

    @Test
    public void
    givenCachePopulated_readsFromTheCache() throws Exception {
        underlyingEventStore.write(stream_1, singletonList(event_1));
        saveAllToCache(getCacheFile("cache.gz"));

        CacheEventReader newCacheEventReader = new CacheEventReader(new JavaInMemoryEventStore(CLOCK), CODEC, cacheDirectory, "cache");
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
        saveAllToCache(getCacheFile("cache.gz"));

        assertThat(readAllFromEmpty(cacheEventReader), hasSize(1));

        CacheEventReader newCacheEventReader = new CacheEventReader(new JavaInMemoryEventStore(CLOCK), CODEC, cacheDirectory, "cache");
        assertThat(readAllFromEmpty(newCacheEventReader), hasSize(1));
        assertThat(readAllFromEmpty(newCacheEventReader), equalTo(readAllToList(underlyingEventStore)));
    }

    @Test
    public void
    givenMultipleCacheFilesInCacheDirectory_allReadInOrder() throws Exception {
        underlyingEventStore.write(stream_1, singletonList(event_1));
        Position firstPosition = saveAllToCache(getCacheFile("cache_1.gz"));

        underlyingEventStore.write(stream_1, singletonList(event_1));
        Position secondPosition = saveAllToCache(getCacheFile("cache_2.gz"), firstPosition);

        underlyingEventStore.write(stream_1, singletonList(event_1));
        Position thirdPosition = saveAllToCache(getCacheFile("cache_3.gz"), secondPosition);

        CacheEventReader newCacheEventReader = new CacheEventReader(new JavaInMemoryEventStore(CLOCK), CODEC, cacheDirectory, "cache");
        List<ResolvedEvent> resolvedEvents = readAllFromEmpty(newCacheEventReader);
        assertThat(resolvedEvents.stream().map(ResolvedEvent::position).collect(toList()), contains(firstPosition, secondPosition, thirdPosition));
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
