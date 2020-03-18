package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.EventRecord;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

@FunctionalInterface
public interface Deserializer<T> {
    void deserialize(EventRecord event, Consumer<? super T> consumer);

    default Deserializer<T> filter(Predicate<? super EventRecord> predicate) {
        return filtering(predicate, this);
    }

    default <U> Deserializer<U> map(BiFunction<? super T, ? super EventRecord, ? extends U> mapper) {
        return wrapping(mapper, this);
    }

    static <T> Deserializer<T> applying(Function<? super EventRecord, ? extends T> function) {
        return new Deserializer<T>() {
            @Override
            public void deserialize(EventRecord event, Consumer<? super T> consumer) {
                consumer.accept(function.apply(event));
            }

            @Override
            public String toString() {
                return "applying(" + function + ")";
            }
        };
    }

    static <T> Deserializer<T> applyingOptional(Function<? super EventRecord, Optional<? extends T>> function) {
        return new Deserializer<T>() {
            @Override
            public void deserialize(EventRecord event, Consumer<? super T> consumer) {
                function.apply(event).ifPresent(consumer);
            }

            @Override
            public String toString() {
                return "applyingOptional(" + function + ")";
            }
        };
    }

    static <T> Deserializer<T> filtering(Predicate<? super EventRecord> predicate, Deserializer<? extends T> downstream) {
        return new Deserializer<T>() {
            @Override
            public void deserialize(EventRecord event, Consumer<? super T> consumer) {
                if (predicate.test(event)) {
                    downstream.deserialize(event, consumer);
                }
            }

            @Override
            public String toString() {
                return "filtering(" + downstream + " with " + predicate + ")";
            }
        };
    }

    static <T, U> Deserializer<U> wrapping(
            BiFunction<? super T, ? super EventRecord, ? extends U> wrapper, Deserializer<? extends T> underlying) {
        requireNonNull(underlying);
        return new Deserializer<U>() {
            @Override
            public void deserialize(EventRecord event, Consumer<? super U> consumer) {
                underlying.deserialize(event, deserialized -> {
                    consumer.accept(wrapper.apply(deserialized, event));
                });
            }

            @Override
            public String toString() {
                return "wrapping(" + underlying + ")";
            }
        };
    }
}
