package com.timgroup.eventstore.merging;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.Position;

import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static java.util.Collections.singleton;

public final class EventShovel {
    private final EventReader reader;
    private final EventSource output;

    private Position currentPosition;

    public EventShovel(EventReader reader, EventSource output) {
        this.reader = reader;
        this.output = output;
        this.currentPosition = reader.emptyStorePosition();
    }

    public void shovelAllNewlyAvailableEvents() {
        reader.readAllForwards(this.currentPosition).forEach(evt -> {
                output.writeStream().write(
                        evt.eventRecord().streamId(),
                        singleton(newEvent(evt.eventRecord().eventType(), evt.eventRecord().data(), evt.eventRecord().metadata()))
                );
                currentPosition = evt.position();
        });
    }
}
