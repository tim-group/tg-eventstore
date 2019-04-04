package com.timgroup.eventsubscription;

import com.lmax.disruptor.dsl.Disruptor;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class EventStoreChaser implements Runnable {
    private final Function<Position, Stream<ResolvedEvent>> eventSource;
    private final Disruptor<EventContainer> disruptor;
    private final ChaserListener listener;
    private final EventContainer.Translator translator = new EventContainer.Translator();

    private Position lastPosition;

    public EventStoreChaser(
            Function<Position, Stream<ResolvedEvent>> eventSource,
            Position startingPosition,
            Disruptor<EventContainer> disruptor,
            ChaserListener listener) {
        this.eventSource = requireNonNull(eventSource);
        this.disruptor = disruptor;
        this.listener = requireNonNull(listener);
        this.lastPosition = requireNonNull(startingPosition);

    }

    @Override
    public void run() {
        try {
            try (Stream<ResolvedEvent> stream = eventSource.apply(lastPosition)) {
                stream.forEachOrdered(nextEvent -> {
                    listener.chaserReceived(nextEvent.position());
                    lastPosition = nextEvent.position();
                    disruptor.publishEvent(translator.setting(nextEvent));
                });
            }

            listener.chaserUpToDate(lastPosition);
        } catch (Exception e) {
            listener.transientFailure(e);
        }
    }
}
