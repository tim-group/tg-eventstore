package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.Position;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public interface EventHandler {
    default void apply(Event deserialized) {
        throw new UnsupportedOperationException();
    }

    default void apply(Position position, Event deserialized) {
        apply(deserialized);
    }

    @Deprecated
    default void apply(Position position, Event deserialized, boolean endOfBatch) {
        apply(position, deserialized);
    }

    static EventHandler concat(EventHandler... handlers) {
        return new BroadcastingEventHandler(Arrays.asList(handlers));
    }

    static EventHandler concatAll(List<EventHandler> handlers) {
        return new BroadcastingEventHandler(handlers);
    }

    static EventHandler ofConsumer(Consumer<? super Event> consumer) {
        Objects.requireNonNull(consumer);

        return new EventHandler() {
            @Override
            public void apply(Event deserialized) {
                consumer.accept(deserialized);
            }

            @Override
            public String toString() {
                return consumer.toString();
            }
        };
    }

    EventHandler DISCARD = ofConsumer(e -> {});
}
