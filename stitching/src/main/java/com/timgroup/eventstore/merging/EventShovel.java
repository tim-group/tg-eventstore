package com.timgroup.eventstore.merging;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;

import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static java.util.Collections.singleton;

public final class EventShovel {
    private final EventReader reader;
    private final EventSource output;

    public EventShovel(EventReader reader, EventSource output) {
        this.reader = reader;
        this.output = output;
    }

    public void shovelAllNewlyAvailableEvents() {
        reader.readAllForwards().forEach(evt ->
                output.writeStream().write(
                        evt.eventRecord().streamId(),
                        singleton(newEvent(evt.eventRecord().eventType(), evt.eventRecord().data(), evt.eventRecord().metadata()))
                ));
    }
}
