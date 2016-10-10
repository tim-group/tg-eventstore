package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.function.Consumer;
import java.util.stream.Stream;

public class EventStoreChaser implements Runnable {
    private final EventReader eventStore;
    private final Consumer<ResolvedEvent> eventHandler;
    private final ChaserListener listener;

    private Position lastPosition;

    public EventStoreChaser(
            EventReader eventStore,
            Position startingPosition,
            Consumer<ResolvedEvent> eventHandler,
            ChaserListener listener) {
        this.eventStore = eventStore;
        this.eventHandler = eventHandler;
        this.listener = listener;

        this.lastPosition = startingPosition;
    }

    @Override
    public void run() {
        try {
            try (Stream<ResolvedEvent> stream = eventStore.readAllForwards(lastPosition)) {
                stream.forEach(nextEvent -> {
                    listener.chaserReceived(nextEvent.position());
                    lastPosition = nextEvent.position();
                    eventHandler.accept(nextEvent);
                });
            }

            listener.chaserUpToDate(lastPosition);
        } catch (Exception e) {
            listener.transientFailure(e);
        }
    }
}
