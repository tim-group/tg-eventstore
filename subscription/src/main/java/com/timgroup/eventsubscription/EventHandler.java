package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventsubscription.lifecycleevents.InitialCatchupCompleted;
import com.timgroup.eventsubscription.lifecycleevents.SubscriptionTerminated;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

@FunctionalInterface
public interface EventHandler {
    void apply(Position position, Event deserialized);

    default EventHandler andThen(EventHandler o) {
        return SequencingEventHandler.flatten(Arrays.asList(this, requireNonNull(o)));
    }

    default EventHandler andThen(Consumer<? super Event> c) {
        return andThen(ofConsumer(c));
    }

    static EventHandler concat(EventHandler... handlers) {
        return SequencingEventHandler.flatten(Arrays.asList(handlers));
    }

    static EventHandler concatAll(List<? extends EventHandler> handlers) {
        return SequencingEventHandler.flatten(handlers);
    }

    static EventHandler ofConsumer(Consumer<? super Event> consumer) {
        requireNonNull(consumer);

        return new EventHandler() {
            @Override
            public void apply(Position position, Event deserialized) {
                consumer.accept(deserialized);
            }

            @Override
            public String toString() {
                return consumer.toString();
            }
        };
    }

    EventHandler DISCARD = (position, event) -> {};

    static EventHandler onInitialCatchupAt(Consumer<? super Position> callback) {
        requireNonNull(callback);

        return (position, event) -> {
            if (event instanceof InitialCatchupCompleted) {
                callback.accept(position);
            }
        };
    }

    static EventHandler onInitialCatchup(Runnable callback) {
        requireNonNull(callback);

        return onInitialCatchupAt(ignored -> callback.run());
    }

    static EventHandler onTermination(BiConsumer<? super Position, ? super Throwable> consumer) {
        requireNonNull(consumer);

        return (position, deserialized) -> {
            if (deserialized instanceof SubscriptionTerminated) {
                consumer.accept(position, ((SubscriptionTerminated) deserialized).exception);
            }
        };
    }
}
