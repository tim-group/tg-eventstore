package com.timgroup.eventstore.memory;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.PositionCodec;

public class InMemoryEventSource implements EventSource {
    private final JavaInMemoryEventStore eventStore;

    public InMemoryEventSource(JavaInMemoryEventStore eventStore) {
        this.eventStore = eventStore;
    }

    @Override
    public EventReader readAll() {
        return eventStore;
    }

    @Override
    public EventCategoryReader readCategory() {
        return eventStore;
    }

    @Override
    public EventStreamReader readStream() {
        return eventStore;
    }

    @Override
    public EventStreamWriter writeStream() {
        return eventStore;
    }

    @Override
    public PositionCodec positionCodec() {
        return eventStore;
    }
}
