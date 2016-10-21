package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.EventRecord;

import java.util.function.Consumer;

public interface Deserializer<T> {
    T deserialize(EventRecord event);

    default void deserialize(EventRecord event, Consumer<T> consumer) {
        consumer.accept(deserialize(event));
    }
}
