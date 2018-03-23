package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.Position;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;

public interface EventHandler<T> {
    default void apply(T deserialized) {}

    default void apply(Position position, DateTime timestamp, T deserialized, boolean endOfBatch) {
        apply(deserialized);
    }

    @SafeVarargs
    static <E> EventHandler<E> concat(EventHandler<E>... handlers) {
        return new BroadcastingEventHandler<>(Arrays.asList(handlers));
    }

    static <E> EventHandler<E> ofConsumer(Consumer<? super E> consumer) {
        Objects.requireNonNull(consumer);

        return new EventHandler<E>() {
            @Override
            public void apply(E deserialized) {
                consumer.accept(deserialized);
            }

            @Override
            public String toString() {
                return consumer.toString();
            }
        };
    }

    static <E> EventHandler<E> discard() {
        return ofConsumer(e -> {});
    }
}
