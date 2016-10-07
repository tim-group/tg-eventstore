package com.timgroup.eventsubscription;

import com.lmax.disruptor.WorkHandler;

public class DisruptorDeserializationAdapter<T> implements WorkHandler<EventContainer<T>> {
    private final Deserializer<T> deserializer;
    private final EventProcessorListener processorListener;

    public DisruptorDeserializationAdapter(Deserializer<T> deserializer, EventProcessorListener processorListener) {
        this.deserializer = deserializer;
        this.processorListener = processorListener;
    }

    @Override
    public void onEvent(EventContainer<T> eventContainer) throws Exception {
        try {
            eventContainer.deserializedEvent = deserializer.deserialize(eventContainer.event);
            processorListener.eventDeserialized(eventContainer.event.position());
        } catch (Exception e) {
            processorListener.eventDeserializationFailed(eventContainer.event.position(), e);
            throw e;
        }
    }
}
