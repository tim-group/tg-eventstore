package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventsubscription.healthcheck.SubscriptionListener;
import com.timgroup.structuredevents.EventSink;
import com.timgroup.structuredevents.LocalEventSink;
import com.timgroup.structuredevents.Slf4jEventSink;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class SubscriptionBuilder<T> {
    private final String name;
    private Clock clock = Clock.systemUTC();
    private Duration runFrequency = Duration.ofSeconds(1);
    private Duration maxInitialReplayDuration = Duration.ofSeconds(1);
    private int bufferSize = 1024;
    private List<EventHandler<T>> handlers = Collections.emptyList();
    private List<SubscriptionListener> listeners = Collections.emptyList();

    private Function<Position, Stream<ResolvedEvent>> reader = null;
    private Position startingPosition = null;
    private Deserializer<T> deserializer = null;
    private EventSink eventSink = new Slf4jEventSink();

    private SubscriptionBuilder(String name) {
        this.name = name;
    }

    public static <T> SubscriptionBuilder<T> eventSubscription(String name) {
        return new SubscriptionBuilder<T>(name);
    }

    public SubscriptionBuilder<T> deserializingUsing(Deserializer<T> deserializer) {
        this.deserializer = deserializer;
        return this;
    }

    public SubscriptionBuilder<T> withClock(Clock clock) {
        this.clock = clock;
        return this;
    }

    public SubscriptionBuilder<T> runningInParallelWithBuffer(int bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }

    public SubscriptionBuilder<T> withRunFrequency(Duration runFrequency) {
        this.runFrequency = runFrequency;
        return this;
    }

    public SubscriptionBuilder<T> withMaxInitialReplayDuration(Duration maxInitialReplayDuration) {
        this.maxInitialReplayDuration = maxInitialReplayDuration;
        return this;
    }

    public SubscriptionBuilder<T> readingFrom(EventReader eventReader) {
        return readingFrom(eventReader, eventReader.emptyStorePosition());
    }

    public SubscriptionBuilder<T> readingFrom(EventReader eventReader, Position startingPosition) {
        this.reader = eventReader::readAllForwards;
        this.startingPosition = startingPosition;
        return this;
    }

    public SubscriptionBuilder<T> readingFrom(EventCategoryReader categoryReader, String category) {
        return readingFrom(categoryReader, category, categoryReader.emptyCategoryPosition(category));
    }

    public SubscriptionBuilder<T> readingFrom(EventCategoryReader categoryReader, String category, Position startingPosition) {
        this.reader = pos -> categoryReader.readCategoryForwards(category, startingPosition);
        this.startingPosition = startingPosition;
        return this;
    }

    public SubscriptionBuilder<T> publishingTo(EventHandler<T>... handlers) {
        this.handlers = Arrays.asList(handlers);
        return this;
    }

    public SubscriptionBuilder<T> publishingTo(Collection<EventHandler<T>> handlers) {
        this.handlers = new ArrayList<>(handlers);
        return this;
    }

    public SubscriptionBuilder<T> withListeners(SubscriptionListener... listeners) {
        this.listeners = Arrays.asList(listeners);
        return this;
    }

    public SubscriptionBuilder<T> withListeners(Collection<SubscriptionListener> listeners) {
        this.listeners = new ArrayList<>(listeners);
        return this;
    }


    public SubscriptionBuilder<T> withEventSink(EventSink eventSink) {
        this.eventSink = eventSink;
        return this;
    }

    public EventSubscription<T> build() {
        requireNonNull(reader);
        requireNonNull(startingPosition);
        requireNonNull(deserializer);

        return new EventSubscription<T>(
                name,
                reader,
                deserializer,
                handlers,
                clock,
                bufferSize,
                runFrequency,
                startingPosition,
                maxInitialReplayDuration,
                listeners,
                eventSink
        );
    }

}
