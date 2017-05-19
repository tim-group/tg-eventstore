package com.timgroup.eventsubscription;

import java.util.Arrays;

import com.timgroup.eventstore.api.Position;
import org.joda.time.DateTime;

public interface EventHandler<T> {
    default void apply(T deserialized) {}

    default void apply(Position position, DateTime timestamp, T deserialized, boolean endOfBatch) {
        apply(deserialized);
    }

    @SafeVarargs
    static <E> EventHandler<E> concat(EventHandler<E>... handlers) {
        return new BroadcastingEventHandler<>(Arrays.asList(handlers));
    }
}
