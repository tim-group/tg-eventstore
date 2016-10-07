package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.EventInStream;

import java.util.List;

public class BroadcastingEventHandler<T> implements EventHandler<T> {
    private final List<EventHandler<T>> handlers;

    public BroadcastingEventHandler(List<EventHandler<T>> handlers) {
        this.handlers = handlers;
    }

    @Override
    public void apply(EventInStream event, T deserialized, boolean endOfBatch) {
        for (EventHandler<T> handler : handlers) {
            handler.apply(event, deserialized, endOfBatch);
        }
    }
}
