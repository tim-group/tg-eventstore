package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public interface EventHandler {
    default void apply(Event deserialized) {
        throw new UnsupportedOperationException();
    }

    default void apply(Position position, DateTime timestamp, Event deserialized, boolean endOfBatch) {
        apply(deserialized);
    }

    default void apply(ResolvedEvent resolvedEvent, Event deserializedEvent, boolean endOfBatch) {
        apply(resolvedEvent.position(), new DateTime(resolvedEvent.eventRecord().timestamp().toEpochMilli()), deserializedEvent, endOfBatch);
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

    static EventHandler discard() {
        return ofConsumer(e -> {});
    }
}
