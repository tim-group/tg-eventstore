package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.Position;

import java.util.ArrayList;
import java.util.List;

final class BroadcastingEventHandler implements EventHandler {
    private final List<EventHandler> handlers;

    BroadcastingEventHandler(List<? extends EventHandler> handlers) {
        this.handlers = new ArrayList<>(handlers);
    }

    @Override
    public void apply(Position position, Event deserialized, boolean endOfBatch) {
        for (EventHandler handler : handlers) {
            handler.apply(position, deserialized, endOfBatch);
        }
    }

    @Override
    public void apply(Position position, Event deserialized) {
        for (EventHandler handler : handlers) {
            handler.apply(position, deserialized);
        }
    }


    @Override
    public void apply(Event deserialized) {
        for (EventHandler handler : handlers) {
            handler.apply(deserialized);
        }
    }
}
