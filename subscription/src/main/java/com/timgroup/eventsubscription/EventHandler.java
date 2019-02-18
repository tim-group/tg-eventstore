package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.Position;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public interface EventHandler<T> {
    default void apply(T deserialized) {
        throw new UnsupportedOperationException();
    }

    default void apply(Position position, DateTime timestamp, T deserialized, boolean endOfBatch) {
        apply(deserialized);
    }

    @SafeVarargs
    static <E> EventHandler<E> concat(EventHandler<? super E>... handlers) {
        return new BroadcastingEventHandler<>(Arrays.asList(handlers));
    }

    static <E> EventHandler<E> concatAll(List<? extends EventHandler<? super E>> handlers) {
        return new BroadcastingEventHandler<>(handlers);
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
