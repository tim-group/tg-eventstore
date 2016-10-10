package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.EventInStream;
import com.timgroup.eventstore.api.Position;
import org.joda.time.DateTime;

public interface EventHandler<T> {
    default void apply(T deserialized) {}

    default void apply(Position position, DateTime timestamp, T deserialized, boolean endOfBatch) {
        apply(deserialized);
    }
}
