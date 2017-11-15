package com.timgroup.eventstore.datastream;

import com.timgroup.eventstore.api.*;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.time.Clock;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.eventstore.api.ObjectPropertiesMatcher.objectWith;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class DataStreamEventReaderTest {
    JavaInMemoryEventStore eventStore = new JavaInMemoryEventStore(Clock.systemUTC());
    EventReader underlying = eventStore;
    DataStreamEventReader dataStreamEventReader = new DataStreamEventReader(underlying);

    private final String category_1 = randomCategory();

    private final StreamId stream_1 = streamId(category_1, "1");

    private final NewEvent event_1 = anEvent();

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
