package com.timgroup.eventsubscription;

import com.lmax.disruptor.WorkHandler;

public class DisruptorDeserializationAdapter<T> implements WorkHandler<EventContainer<T>> {
    private final Deserializer<? extends T> deserializer;
    private final EventProcessorListener processorListener;

    public DisruptorDeserializationAdapter(Deserializer<? extends T> deserializer, EventProcessorListener processorListener) {
        this.deserializer = deserializer;
        this.processorListener = processorListener;
    }

    @Override
    public void onEvent(EventContainer<T> eventContainer) throws Exception {
        try {
            eventContainer.deserializedEvent = null;
            deserializer.deserialize(eventContainer.event.eventRecord(), evt -> {
                eventContainer.deserializedEvent = evt;
            });
            processorListener.eventDeserialized(eventContainer.event.position());
        } catch (Exception e) {
            processorListener.eventDeserializationFailed(eventContainer.event.position(), e);
            throw e;
        }
    }
}
