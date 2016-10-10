package com.timgroup.eventsubscription;

import org.joda.time.DateTime;

import java.time.Instant;

public class DisruptorEventHandlerAdapter<T> implements com.lmax.disruptor.EventHandler<EventContainer<T>> {
    private final EventHandler<T> eventHandler;
    private final EventProcessorListener processorListener;

    public DisruptorEventHandlerAdapter(EventHandler<T> eventHandler, EventProcessorListener processorListener) {
        this.eventHandler = eventHandler;
        this.processorListener = processorListener;
    }

    @Override
    public void onEvent(EventContainer<T> eventContainer, long sequence, boolean endOfBatch) throws Exception {
        try {
            Instant timestamp = eventContainer.event.eventRecord().timestamp();
            eventHandler.apply(eventContainer.event.position(), new DateTime(timestamp.toEpochMilli()), eventContainer.deserializedEvent, endOfBatch);
            processorListener.eventProcessed(eventContainer.event.position());
        } catch (Exception e) {
            processorListener.eventProcessingFailed(eventContainer.event.position(), e);
            throw e;
        }
    }
}
