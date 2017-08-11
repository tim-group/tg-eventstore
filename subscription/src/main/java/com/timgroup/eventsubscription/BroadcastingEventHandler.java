package com.timgroup.eventsubscription;

import java.util.List;

import com.timgroup.eventstore.api.Position;
import org.joda.time.DateTime;

public class BroadcastingEventHandler<T> implements EventHandler<T> {
    private final List<EventHandler<? super T>> handlers;

    public BroadcastingEventHandler(List<EventHandler<? super T>> handlers) {
        this.handlers = handlers;
    }

    @Override
    public void apply(Position position, DateTime timestamp, T deserialized, boolean endOfBatch) {
        for (EventHandler<? super T> handler : handlers) {
            handler.apply(position, timestamp, deserialized, endOfBatch);
        }
    }
}
