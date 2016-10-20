package com.timgroup.eventstore.memory;

import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.JavaEventStoreTest;

import java.time.Clock;

public class JavaInMemoryEventStoreTest extends JavaEventStoreTest {

    private final JavaInMemoryEventStore eventStore = new JavaInMemoryEventStore(Clock.systemUTC());

    @Override
    public EventSource eventSource() {
        return new InMemoryEventSource(eventStore);
    }
}