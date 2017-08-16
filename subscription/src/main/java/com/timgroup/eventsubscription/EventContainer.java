package com.timgroup.eventsubscription;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventTranslator;
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

    public static class Translator<T> implements EventTranslator<EventContainer<T>> {
        private ResolvedEvent currentEvent;

        public Translator<T> setting(ResolvedEvent event) {
            currentEvent = event;
            return this;
        }

        @Override
        public void translateTo(EventContainer<T> eventContainer, long sequence) {
            eventContainer.event = currentEvent;
        }
    }

    @Override
    public String toString() {
        return "EventContainer{" +
                "event=" + event +
                ", deserializedEvent=" + deserializedEvent +
                '}';
    }
}
