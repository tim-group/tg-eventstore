package com.timgroup.eventstore.datastream;

import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class CachingEventReaderTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    Path cacheDirectory;

    CachingEventReader cachingEventReader;

    JavaInMemoryEventStore underlyingEventStore = new JavaInMemoryEventStore(Clock.systemUTC());

    private final String category_1 = randomCategory();

    private final StreamId stream_1 = streamId(category_1, "1");

    private final NewEvent event_1 = anEvent();

    @Before public void init() throws IOException {
        temporaryFolder.create();
        cacheDirectory = temporaryFolder.getRoot().toPath();
        cachingEventReader = new CachingEventReader(underlyingEventStore, JavaInMemoryEventStore.CODEC, cacheDirectory);
    }

    @Test
    public void
    givenNoDataInUnderlying_returnsNothing() {
        assertThat(cachingEventReader.readAllForwards().collect(Collectors.toList()), Matchers.empty());
    }

    @Test
    public void
    givenAnEventDataInUnderlying_returnsThatEvent() {
        underlyingEventStore.write(stream_1, asList(event_1));

        assertThat(cachingEventReader.readAllForwards().collect(toList()), hasSize(1));

        assertThat(cachingEventReader.readAllForwards().collect(toList()),
                equalTo(underlyingEventStore.readAllForwards().collect(toList())));
    }

    @Test
    public void
    givenAlreadyReadAllOnce_readsFromTheCache() {
        underlyingEventStore.write(stream_1, asList(event_1));

        Stream<ResolvedEvent> resolvedEventStream = cachingEventReader.readAllForwards();
        assertThat(resolvedEventStream.collect(toList()), hasSize(1));

        CachingEventReader newCachingEventReader = new CachingEventReader(new JavaInMemoryEventStore(Clock.systemUTC()),  JavaInMemoryEventStore.CODEC, cacheDirectory);

        assertThat(newCachingEventReader.readAllForwards().collect(toList()), hasSize(1));

        assertThat(newCachingEventReader.readAllForwards().collect(toList()),
                equalTo(underlyingEventStore.readAllForwards().collect(toList())));
    }

    @Test
    public void
    givenReadingFromEmptyStorePosition_readsFromTheCache() {
        underlyingEventStore.write(stream_1, asList(event_1));

        Stream<ResolvedEvent> resolvedEventStream = cachingEventReader.readAllForwards(underlyingEventStore.emptyStorePosition());
        assertThat(resolvedEventStream.collect(toList()), hasSize(1));

        CachingEventReader newCachingEventReader = new CachingEventReader(new JavaInMemoryEventStore(Clock.systemUTC()),  JavaInMemoryEventStore.CODEC, cacheDirectory);

        assertThat(newCachingEventReader.readAllForwards(underlyingEventStore.emptyStorePosition()).collect(toList()), hasSize(1));

        assertThat(newCachingEventReader.readAllForwards(underlyingEventStore.emptyStorePosition()).collect(toList()),
                equalTo(underlyingEventStore.readAllForwards().collect(toList())));
    }


    private static NewEvent anEvent() {
        return newEvent(UUID.randomUUID().toString(), randomData(), randomData());
    }

    private static String randomCategory() {
        return "stream_" + UUID.randomUUID().toString().replace("-", "");
    }

    private static byte[] randomData() {
        return ("{\n  \"value\": \"" + UUID.randomUUID() + "\"\n}").getBytes(UTF_8);
    }
}
