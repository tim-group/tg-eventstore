package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

public class EventStoreChaser implements Runnable {
    private final Function<Position, Stream<ResolvedEvent>> eventSource;
    private final Consumer<ResolvedEvent> eventHandler;
    private final ChaserListener listener;

    private Position lastPosition;

    public EventStoreChaser(
            Function<Position, Stream<ResolvedEvent>> eventSource,
            Position startingPosition,
            Consumer<ResolvedEvent> eventHandler,
            ChaserListener listener) {
        this.eventSource = eventSource;
        this.eventHandler = eventHandler;
        this.listener = listener;
        this.lastPosition = startingPosition;
    }

    @Override
    public void run() {
        try {
            try (Stream<ResolvedEvent> stream = eventSource.apply(lastPosition)) {
                stream.forEachOrdered(nextEvent -> {
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
