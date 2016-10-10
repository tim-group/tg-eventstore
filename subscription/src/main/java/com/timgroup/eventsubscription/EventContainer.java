package com.timgroup.eventsubscription;

import com.lmax.disruptor.EventFactory;
import com.timgroup.eventstore.api.ResolvedEvent;

public class EventContainer<T> {
    ResolvedEvent event = null;
    T deserializedEvent = null;

    public static class Factory<T> implements EventFactory<EventContainer<T>> {
        @Override
        public EventContainer<T> newInstance() {
            return new EventContainer<>();
        }
    }
}
