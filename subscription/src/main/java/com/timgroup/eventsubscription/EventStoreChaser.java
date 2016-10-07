package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.EventInStream;
import com.timgroup.eventstore.api.EventStore;
import com.timgroup.eventstore.api.LegacyPositionAdapter;
import com.timgroup.eventstore.api.Position;

import java.util.function.Consumer;
import java.util.stream.Stream;

public class EventStoreChaser implements Runnable {
    private final EventStore eventStore;
    private final Consumer<EventInStream> eventHandler;
    private final ChaserListener listener;

    private Position lastPosition;

    public EventStoreChaser(
            EventStore eventStore,
            Position startingPosition,
            Consumer<EventInStream> eventHandler,
            ChaserListener listener) {
        this.eventStore = eventStore;
        this.eventHandler = eventHandler;
        this.listener = listener;

        this.lastPosition = startingPosition;
    }

    @Override
    public void run() {
        try {
            try (Stream<EventInStream> stream = eventStore.streamingFromAll(((LegacyPositionAdapter) lastPosition).version())) {
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
