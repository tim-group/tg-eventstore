package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;

final class BroadcastingEventHandler implements EventHandler {
    private final List<EventHandler> handlers;

    BroadcastingEventHandler(List<? extends EventHandler> handlers) {
        this.handlers = new ArrayList<>(handlers);
    }

    @Override
    public void apply(ResolvedEvent resolvedEvent, Event deserializedEvent, boolean endOfBatch) {
        for (EventHandler handler : handlers) {
            handler.apply(resolvedEvent, deserializedEvent, endOfBatch);
        }
    }

    @Override
    public void apply(Position position, DateTime timestamp, Event deserialized, boolean endOfBatch) {
        for (EventHandler handler : handlers) {
            handler.apply(position, timestamp, deserialized, endOfBatch);
        }
    }

    @Override
    public void apply(Event deserialized) {
        for (EventHandler handler : handlers) {
            handler.apply(deserialized);
        }
    }
}
