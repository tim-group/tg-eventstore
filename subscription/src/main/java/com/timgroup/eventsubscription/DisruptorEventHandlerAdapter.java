package com.timgroup.eventsubscription;

import com.codahale.metrics.Counter;
import com.timgroup.eventsubscription.lifecycleevents.CaughtUp;
import com.timgroup.eventsubscription.lifecycleevents.SubscriptionTerminated;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DisruptorEventHandlerAdapter implements com.lmax.disruptor.EventHandler<EventContainer> {
    private final EventHandler eventHandler;
    private final Optional<Counter> missedCatchupCounter;

    public DisruptorEventHandlerAdapter(EventHandler eventHandler, Optional<Counter> missedCatchupCounter) {
        this.eventHandler = requireNonNull(eventHandler);
        this.missedCatchupCounter = missedCatchupCounter;
    }

    @Override
    public void onEvent(EventContainer eventContainer, long sequence, boolean endOfBatch) throws Exception {
        if (eventContainer.deserializedEvent instanceof CaughtUp && !endOfBatch) {
            missedCatchupCounter.ifPresent(Counter::inc);
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
