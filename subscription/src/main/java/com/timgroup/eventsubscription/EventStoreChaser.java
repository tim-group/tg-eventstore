package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.EventInStream;
import com.timgroup.eventstore.api.EventStore;

import java.util.function.Consumer;
import java.util.stream.Stream;

public class EventStoreChaser implements Runnable {
    private final EventStore eventStore;
    private final Consumer<EventInStream> eventHandler;
    private final ChaserListener listener;

    private long lastVersion;

    public EventStoreChaser(
            EventStore eventStore,
            long fromVersion,
            Consumer<EventInStream> eventHandler,
            ChaserListener listener) {
        this.eventStore = eventStore;
        this.eventHandler = eventHandler;
        this.listener = listener;

        this.lastVersion = fromVersion;
    }

    @Override
    public void run() {
        try {
            try (Stream<EventInStream> stream = eventStore.streamingFromAll(lastVersion)) {
                stream.forEach(nextEvent -> {
                    listener.chaserReceived(nextEvent.version());
                    lastVersion = nextEvent.version();
                    eventHandler.accept(nextEvent);
                });
            }

            listener.chaserUpToDate(lastVersion);
        } catch (Exception e) {
            listener.transientFailure(e);
        }
    }
}
