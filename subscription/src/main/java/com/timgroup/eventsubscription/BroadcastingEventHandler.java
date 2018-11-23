package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.Position;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;

final class BroadcastingEventHandler<T> implements EventHandler<T> {
    private final List<EventHandler<? super T>> handlers;

    BroadcastingEventHandler(List<EventHandler<? super T>> handlers) {
        this.handlers = new ArrayList<>(handlers);
    }

    @Override
    public void apply(Position position, DateTime timestamp, T deserialized, boolean endOfBatch) {
        for (EventHandler<? super T> handler : handlers) {
            handler.apply(position, timestamp, deserialized, endOfBatch);
        }
    }

    @Override
    public void apply(T deserialized) {
        for (EventHandler<? super T> handler : handlers) {
            handler.apply(deserialized);
        }
    }
}
