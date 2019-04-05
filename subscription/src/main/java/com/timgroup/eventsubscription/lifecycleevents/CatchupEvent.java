package com.timgroup.eventsubscription.lifecycleevents;

import com.timgroup.eventstore.api.Position;

import java.time.Instant;

public interface CatchupEvent extends SubscriptionLifecycleEvent {
    Position position();

    Instant timestamp();
}
