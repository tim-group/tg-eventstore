package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventsubscription.lifecycleevents.InitialCatchupCompleted;
import com.timgroup.eventsubscription.lifecycleevents.SubscriptionTerminated;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public interface EventHandler {
    default void apply(Event deserialized) {
        throw new UnsupportedOperationException();
    }

    default void apply(Position position, Event deserialized) {
        apply(deserialized);
    }

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


    static EventHandler onInitialCatchupAt(Consumer<? super Position> callback) {
        return new EventHandler() {
            @Override
            public void apply(Position position, Event event) {
                if (event instanceof InitialCatchupCompleted) {
                    callback.accept(position);
                }
            }
        };
    }

    static EventHandler onInitialCatchup(Runnable callback) {
        return onInitialCatchupAt(ignored -> callback.run());
    }

    static EventHandler onTermination(BiConsumer<? super Position, ? super Throwable> consumer) {
        return new EventHandler() {
            @Override
            public void apply(Position position, Event deserialized) {
                if (deserialized instanceof SubscriptionTerminated) {
                    consumer.accept(position, ((SubscriptionTerminated) deserialized).exception);
                }
            }
        };
    }
}
