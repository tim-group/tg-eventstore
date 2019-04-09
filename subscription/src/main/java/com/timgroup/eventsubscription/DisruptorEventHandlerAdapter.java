package com.timgroup.eventsubscription;

import com.timgroup.eventsubscription.lifecycleevents.CaughtUp;
import com.timgroup.eventsubscription.lifecycleevents.SubscriptionTerminated;

import static java.util.Objects.requireNonNull;

public class DisruptorEventHandlerAdapter implements com.lmax.disruptor.EventHandler<EventContainer> {
    private final EventHandler eventHandler;

    public DisruptorEventHandlerAdapter(EventHandler eventHandler) {
        this.eventHandler = requireNonNull(eventHandler);
    }

    @Override
    public void onEvent(EventContainer eventContainer, long sequence, boolean endOfBatch) throws Exception {
        if (eventContainer.deserializedEvent instanceof CaughtUp && !endOfBatch) {
            clear(eventContainer);
            return;
        }

        try {
            if (eventContainer.deserializedEvent != null) {
                if (eventContainer.deserializedEvent instanceof SubscriptionTerminated) {
                    throw ((SubscriptionTerminated) eventContainer.deserializedEvent).exception;
                }

                eventHandler.apply(eventContainer.position, eventContainer.deserializedEvent);
            }

            clear(eventContainer);
        } catch (Exception e) {
            eventHandler.apply(eventContainer.position, new SubscriptionTerminated(eventContainer.position, e));
            throw e;
        }
    }

    private void clear(EventContainer eventContainer) {
        eventContainer.event = null;
        eventContainer.deserializedEvent = null;
        eventContainer.position = null;
    }
}
