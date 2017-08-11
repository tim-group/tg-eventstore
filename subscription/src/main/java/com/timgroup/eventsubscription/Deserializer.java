package com.timgroup.eventsubscription;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.timgroup.eventstore.api.EventRecord;

public interface Deserializer<T> {
    T deserialize(EventRecord event);

    default void deserialize(EventRecord event, Consumer<T> consumer) {
        consumer.accept(deserialize(event));
    }

    static <T> Deserializer<T> applying(Function<? super EventRecord, ? extends T> function) {
        return function::apply;
    }

    static <T> Deserializer<T> filtering(Predicate<? super EventRecord> predicate, Deserializer<? extends T> downstream) {
        return new Deserializer<T>() {
            @Override
            public void deserialize(EventRecord event, Consumer<T> consumer) {
                if (predicate.test(event)) {
                    consumer.accept(downstream.deserialize(event));
                }
            }

            @Override
            public T deserialize(EventRecord event) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
