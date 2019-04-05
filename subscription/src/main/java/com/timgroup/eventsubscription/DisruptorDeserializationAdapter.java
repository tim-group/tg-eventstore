package com.timgroup.eventsubscription;

import com.lmax.disruptor.WorkHandler;
import com.timgroup.eventsubscription.lifecycleevents.SubscriptionTerminated;

import static java.util.Objects.requireNonNull;

public class DisruptorDeserializationAdapter implements WorkHandler<EventContainer> {
    private final Deserializer<? extends Event> deserializer;

    public DisruptorDeserializationAdapter(Deserializer<? extends Event> deserializer) {
        this.deserializer = requireNonNull(deserializer);
    }

    @Override
    public void onEvent(EventContainer eventContainer) {
        if (eventContainer.deserializedEvent != null) {
            return;
        }
        try {
            deserializer.deserialize(eventContainer.event.eventRecord(), evt -> eventContainer.deserializedEvent = evt);
        } catch (Exception e) {
            eventContainer.deserializedEvent = new SubscriptionTerminated(eventContainer.position, e);
        }
    }
}
