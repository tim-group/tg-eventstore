package com.timgroup.eventsubscription;

import static java.util.Objects.requireNonNull;

public class DisruptorEventHandlerAdapter<T> implements com.lmax.disruptor.EventHandler<EventContainer<T>> {
    private final EventHandler<? super T> eventHandler;
    private final EventProcessorListener processorListener;

    public DisruptorEventHandlerAdapter(EventHandler<? super T> eventHandler, EventProcessorListener processorListener) {
        this.eventHandler = requireNonNull(eventHandler);
        this.processorListener = requireNonNull(processorListener);
    }

    @Override
    public void onEvent(EventContainer<T> eventContainer, long sequence, boolean endOfBatch) throws Exception {
        try {
            if (eventContainer.deserializedEvent != null) {
                eventHandler.apply(eventContainer.event, eventContainer.deserializedEvent, endOfBatch);
            }
            processorListener.eventProcessed(eventContainer.event.position());

            eventContainer.event = null;
            eventContainer.deserializedEvent = null;
        } catch (Exception e) {
            processorListener.eventProcessingFailed(eventContainer.event.position(), e);
            throw e;
        }
    }
}
