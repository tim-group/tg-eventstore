package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.Position;
import org.joda.time.DateTime;

public interface EventHandler<T> {
    default void apply(T deserialized) {}

    default void apply(Position position, DateTime timestamp, T deserialized, boolean endOfBatch) {
        apply(deserialized);
    }

    @SafeVarargs
    static <E> EventHandler<E> concat(EventHandler<E>... handlers) {
        return new EventHandler<E>() {
            @Override
            public void apply(E deserialized) {
                for (EventHandler<E> handler : handlers) {
                    handler.apply(deserialized);
                }
            }

            @Override
            public void apply(Position position, DateTime timestamp, E deserialized, boolean endOfBatch) {
                for (EventHandler<E> handler : handlers) {
                    handler.apply(position, timestamp, deserialized, endOfBatch);
                }
            }
        };
    }
}
