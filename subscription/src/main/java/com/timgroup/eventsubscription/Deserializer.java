package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.EventRecord;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public interface Deserializer<T> {
    @Nonnull T deserialize(EventRecord event);

    default void deserialize(EventRecord event, Consumer<? super T> consumer) {
        consumer.accept(deserialize(event));
    }

    static <T> Deserializer<T> applying(Function<? super EventRecord, ? extends T> function) {
        return t -> requireNonNull(function.apply(t));
    }

    static <T> Deserializer<T> applyingOptional(Function<? super EventRecord, Optional<? extends T>> function) {
        return new Deserializer<T>() {
            @Nonnull
            @Override
            public T deserialize(EventRecord event) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void deserialize(EventRecord event, Consumer<? super T> consumer) {
                function.apply(event).ifPresent(consumer);
            }
        };
    }

    static <T> Deserializer<T> filtering(Predicate<? super EventRecord> predicate, Deserializer<? extends T> downstream) {
        return new Deserializer<T>() {
            @Override
            public void deserialize(EventRecord event, Consumer<? super T> consumer) {
                if (predicate.test(event)) {
                    consumer.accept(downstream.deserialize(event));
                }
            }

            @Nonnull
            @Override
            public T deserialize(EventRecord event) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
