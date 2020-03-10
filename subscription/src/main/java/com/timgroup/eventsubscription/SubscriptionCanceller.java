package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.Position;

@FunctionalInterface
public interface SubscriptionCanceller {
    enum Signal { CONTINUE, CANCEL_INCLUSIVE, CANCEL_EXCLUSIVE }

    Signal cancel(Position position, Event event);
}
