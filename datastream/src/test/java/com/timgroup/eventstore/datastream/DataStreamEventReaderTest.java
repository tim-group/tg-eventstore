package com.timgroup.eventstore.datastream;

import com.timgroup.eventstore.api.*;
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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class DataStreamEventReaderTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    Path cacheDirectory;

    DataStreamEventReader dataStreamEventReader;

    JavaInMemoryEventStore eventStore = new JavaInMemoryEventStore(Clock.systemUTC());
    EventReader underlying = eventStore;

    private final String category_1 = randomCategory();

    private final StreamId stream_1 = streamId(category_1, "1");

    private final NewEvent event_1 = anEvent();

    public DataStreamEventReaderTest() {
    }

    @Before public void init() throws IOException {
        temporaryFolder.create();
        cacheDirectory = temporaryFolder.getRoot().toPath();
        dataStreamEventReader = new DataStreamEventReader(underlying, JavaInMemoryEventStore.CODEC, cacheDirectory);
    }

    @Test
    public void
    givenNoDataInUnderlying_returnsNothing() {
        assertThat(dataStreamEventReader.readAllForwards().collect(Collectors.toList()), Matchers.empty());
    }

    @Test
    public void
    givenAnEventDataInUnderlying_returnsThatEvent() {
        eventStore.write(stream_1, asList(event_1));

        assertThat(dataStreamEventReader.readAllForwards().collect(toList()), hasSize(1));

        assertThat(dataStreamEventReader.readAllForwards().collect(toList()),
                equalTo(eventStore.readAllForwards().collect(toList())));
    }

    @Test
    public void
    givenAlreadyReadAllOnce_readsFromTheCache() {
        eventStore.write(stream_1, asList(event_1));

        Stream<ResolvedEvent> resolvedEventStream = dataStreamEventReader.readAllForwards();
        assertThat(resolvedEventStream.collect(toList()), hasSize(1));

        DataStreamEventReader newDataStreamEventReader = new DataStreamEventReader(new JavaInMemoryEventStore(Clock.systemUTC()),  JavaInMemoryEventStore.CODEC, cacheDirectory);

        assertThat(newDataStreamEventReader.readAllForwards().collect(toList()), hasSize(1));

        assertThat(newDataStreamEventReader.readAllForwards().collect(toList()),
                equalTo(eventStore.readAllForwards().collect(toList())));
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
