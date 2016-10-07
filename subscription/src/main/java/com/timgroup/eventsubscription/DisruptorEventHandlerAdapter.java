package com.timgroup.eventsubscription;

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
            eventHandler.apply(eventContainer.event, eventContainer.deserializedEvent, endOfBatch);
            processorListener.eventProcessed(eventContainer.event.position());
        } catch (Exception e) {
            processorListener.eventProcessingFailed(eventContainer.event.position(), e);
            throw e;
        }
    }
}
