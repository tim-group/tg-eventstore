package com.timgroup.eventsubscription;

import com.lmax.disruptor.dsl.Disruptor;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventsubscription.lifecycleevents.CaughtUp;
import com.timgroup.eventsubscription.lifecycleevents.InitialCatchupCompleted;

import java.time.Clock;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class EventStoreChaser implements Runnable {
    private final Function<Position, Stream<ResolvedEvent>> eventSource;
    private final Disruptor<EventContainer> disruptor;
    private final ChaserListener listener;
    private final EventContainer.Translator translator = new EventContainer.Translator();

    private Position lastPosition;
    private final Clock clock;
    private boolean initialCatchupCompleted;

    public EventStoreChaser(
            Function<Position, Stream<ResolvedEvent>> eventSource,
            Position startingPosition,
            Disruptor<EventContainer> disruptor,
            ChaserListener listener,
            Clock clock) {
        this.eventSource = requireNonNull(eventSource);
        this.disruptor = disruptor;
        this.listener = requireNonNull(listener);
        this.lastPosition = requireNonNull(startingPosition);
        this.clock = clock;
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

            if (initialCatchupCompleted) {
                disruptor.publishEvent((event, sequence) -> {
                    event.deserializedEvent = new CaughtUp(lastPosition, clock.instant());
                    event.position = lastPosition;
                });
            } else {
                initialCatchupCompleted = true;
                disruptor.publishEvent((event, sequence) -> {
                    event.deserializedEvent = new InitialCatchupCompleted(lastPosition, clock.instant());
                    event.position = lastPosition;
                });
            }
            listener.chaserUpToDate(lastPosition);
        } catch (Exception e) {
            listener.transientFailure(e);
        }
    }
}
