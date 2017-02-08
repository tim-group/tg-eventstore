package com.timgroup.eventstore;

import com.timgroup.clocks.testing.ManualClock;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.memory.InMemoryEventSource;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;

public final class MemoryFactoryTest {

    private final ManualClock clock = new ManualClock(Instant.parse("2009-04-12T22:12:32Z"), ZoneId.of("UTC"));
    private final JavaInMemoryEventStore inputReader = new JavaInMemoryEventStore(clock);
    private final InMemoryEventSource inputSource = new InMemoryEventSource(inputReader);

    @Test public void
    can_instantiate_an_inmemory_es_from_a_classpath_resource() throws Exception {
       EventSource fromResource = MemoryFactory.fromResource("/earnings.json.gz");
      assertThat(fromResource.readAll().readAllForwards().collect(Collectors.toList()).size(), Matchers.equalTo(461613));
    }

}