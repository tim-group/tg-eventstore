package com.timgroup.eventstore.memory;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.JavaEventStoreTest;

import java.time.Clock;

public class JavaInMemoryEventStoreTest extends JavaEventStoreTest {

    private final JavaInMemoryEventStore eventStore = new JavaInMemoryEventStore(Clock.systemUTC());

    @Override
    public EventStreamWriter writer() {
        return eventStore;
    }

    @Override
    public EventStreamReader streamEventReader() {
        return eventStore;
    }

    @Override
    public EventReader allEventReader() {
        return eventStore;
    }

    @Override
    public EventCategoryReader eventByCategoryReader() {
        return eventStore;
    }
}