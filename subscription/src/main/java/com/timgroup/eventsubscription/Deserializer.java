package com.timgroup.eventsubscription;

import java.util.function.Consumer;

import com.timgroup.eventstore.api.EventRecord;

public interface Deserializer<T> {
    T deserialize(EventRecord event);

    default void deserialize(EventRecord event, Consumer<T> consumer) {
        consumer.accept(deserialize(event));
    }

}
