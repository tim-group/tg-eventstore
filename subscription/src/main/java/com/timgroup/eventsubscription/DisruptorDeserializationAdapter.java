package com.timgroup.eventsubscription;

import com.lmax.disruptor.WorkHandler;

import static java.util.Objects.requireNonNull;

public class DisruptorDeserializationAdapter implements WorkHandler<EventContainer> {
    private final Deserializer<? extends Event> deserializer;
    private final EventProcessorListener processorListener;

    public DisruptorDeserializationAdapter(Deserializer<? extends Event> deserializer, EventProcessorListener processorListener) {
        this.deserializer = requireNonNull(deserializer);
        this.processorListener = requireNonNull(processorListener);
    }

    @Override
    public void onEvent(EventContainer eventContainer) {
        if (eventContainer.deserializedEvent != null) {
            return;
        }
        try {
            deserializer.deserialize(eventContainer.event.eventRecord(), evt -> {
                eventContainer.deserializedEvent = evt;
            });
        } catch (Exception e) {
            processorListener.eventDeserializationFailed(eventContainer.event.position(), e);
            throw e;
        }
    }
}
