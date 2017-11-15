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

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.stream.Stream;

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

public class CachingEventReaderTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    Path cacheDirectory;

    CachingEventReader cachingEventReader;

    JavaInMemoryEventStore underlyingEventStore = new JavaInMemoryEventStore(Clock.systemUTC());

    private final StreamId stream_1 = streamId(randomCategory(), "1");
    private final NewEvent event_1 = anEvent();

    @Before public void init() throws IOException {
        temporaryFolder.create();
        cacheDirectory = temporaryFolder.getRoot().toPath();
        cachingEventReader = new CachingEventReader(underlyingEventStore, CODEC, cacheDirectory);
    }

    private List<ResolvedEvent> readAllToList(EventReader eventReader) {
        return eventReader.readAllForwards().collect(toList());
    }

    @Test
    public void
    givenNoDataInUnderlying_returnsNothing() {
        assertThat(readAllToList(cachingEventReader), is(empty()));
    }

    @Test
    public void
    givenAnEventDataInUnderlying_returnsThatEvent() {
        underlyingEventStore.write(stream_1, singletonList(event_1));

        assertThat(readAllToList(cachingEventReader), hasSize(1));
        assertThat(readAllToList(cachingEventReader), equalTo(readAllToList(underlyingEventStore)));
    }

    @Test
    public void
    givenAlreadyReadAllOnce_readsFromTheCache() {
        underlyingEventStore.write(stream_1, singletonList(event_1));

        Stream<ResolvedEvent> resolvedEventStream = cachingEventReader.readAllForwards();
        assertThat(resolvedEventStream.collect(toList()), hasSize(1));

        CachingEventReader newCachingEventReader = new CachingEventReader(new JavaInMemoryEventStore(Clock.systemUTC()),  CODEC, cacheDirectory);
        assertThat(readAllToList(newCachingEventReader), hasSize(1));
        assertThat(readAllToList(newCachingEventReader), equalTo(readAllToList(underlyingEventStore)));
    }

    private List<ResolvedEvent> readAllFromEmpty(EventReader eventReader) {
        return eventReader.readAllForwards(underlyingEventStore.emptyStorePosition()).collect(toList());
    }

    @Test
    public void
    givenReadingFromEmptyStorePosition_readsFromTheCache() {
        underlyingEventStore.write(stream_1, singletonList(event_1));
        assertThat(readAllFromEmpty(cachingEventReader), hasSize(1));

        CachingEventReader newCachingEventReader = new CachingEventReader(new JavaInMemoryEventStore(Clock.systemUTC()),  CODEC, cacheDirectory);
        assertThat(readAllFromEmpty(newCachingEventReader), hasSize(1));
        assertThat(readAllFromEmpty(newCachingEventReader), equalTo(readAllToList(underlyingEventStore)));
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
