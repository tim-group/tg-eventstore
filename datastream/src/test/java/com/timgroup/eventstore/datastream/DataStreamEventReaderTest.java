package com.timgroup.eventstore.datastream;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.time.Clock;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;

public class DataStreamEventReaderTest {

    JavaInMemoryEventStore eventStore = new JavaInMemoryEventStore(Clock.systemUTC());
    EventReader underlying = eventStore;
    DataStreamEventReader dataStreamEventReader = new DataStreamEventReader(underlying);

    @Test
    public void
    givenNoDataInUnderlying_returnsNothing() {
        assertThat(dataStreamEventReader.readAllForwards().collect(Collectors.toList()), Matchers.empty());
    }
}
