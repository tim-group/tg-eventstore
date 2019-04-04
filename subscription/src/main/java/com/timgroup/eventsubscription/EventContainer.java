package com.timgroup.eventsubscription;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventTranslator;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;

public class EventContainer {
    ResolvedEvent event = null;
    Position position = null;
    Event deserializedEvent = null;

    public static class Factory implements EventFactory<EventContainer> {
        @Override
        public EventContainer newInstance() {
            return new EventContainer();
        }
    }

    public static class Translator implements EventTranslator<EventContainer> {
        private ResolvedEvent currentEvent;

        public Translator setting(ResolvedEvent event) {
            currentEvent = event;
            return this;
        }

        @Override
        public void translateTo(EventContainer eventContainer, long sequence) {
            eventContainer.event = currentEvent;
            eventContainer.position = currentEvent.position();
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
