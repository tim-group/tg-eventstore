package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.EventInStream;

public interface EventHandler<T> {
    void apply(EventInStream event, T deserialized, boolean endOfBatch);
}
