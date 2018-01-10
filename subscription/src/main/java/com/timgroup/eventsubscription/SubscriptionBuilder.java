package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventsubscription.healthcheck.DurationThreshold;
import com.timgroup.eventsubscription.healthcheck.SubscriptionListener;
import com.timgroup.structuredevents.EventSink;
import com.timgroup.structuredevents.Slf4jEventSink;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class SubscriptionBuilder<T> {
    private final String name;
    private Clock clock = Clock.systemUTC();
    private Duration runFrequency = Duration.ofSeconds(1);
    private DurationThreshold initialReplay = new DurationThreshold(Duration.ofSeconds(1), Duration.ofSeconds(2));
    private DurationThreshold staleness = new DurationThreshold(Duration.ofSeconds(1), Duration.ofSeconds(30));
    private int bufferSize = 1024;
    private final List<EventHandler<? super T>> handlers = new ArrayList<>();
    private final List<SubscriptionListener> listeners = new ArrayList<>();

    private Function<Position, Stream<ResolvedEvent>> reader = null;
    private Position startingPosition = null;
    private Deserializer<? extends T> deserializer = null;
    private EventSink eventSink = new Slf4jEventSink();
    private String readerDescription = null;

    private SubscriptionBuilder(String name) {
        this.name = name;
    }

    public static <T> SubscriptionBuilder<T> eventSubscription(String name) {
        return new SubscriptionBuilder<>(name);
    }

    public SubscriptionBuilder<T> deserializingUsing(Deserializer<? extends T> deserializer) {
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
        this.initialReplay = DurationThreshold.warningThresholdWithCriticalRatio(maxInitialReplayDuration, 1.25);
        return this;
    }

    public SubscriptionBuilder<T> withMaxInitialReplayDuration(DurationThreshold initialReplay) {
        this.initialReplay = initialReplay;
        return this;
    }

    public SubscriptionBuilder<T> withStalenessThreshold(DurationThreshold threshold) {
        this.staleness = threshold;
        return this;
    }

    public SubscriptionBuilder<T> readingFrom(EventReader eventReader) {
        return readingFrom(eventReader, eventReader.emptyStorePosition());
    }

    public SubscriptionBuilder<T> readingFrom(EventReader eventReader, Position startingPosition) {
        this.reader = eventReader::readAllForwards;
        this.readerDescription = EventSubscription.descriptionFor(eventReader);
        this.startingPosition = startingPosition;
        return this;
    }

    public SubscriptionBuilder<T> readingFrom(EventCategoryReader categoryReader, String category) {
        return readingFrom(categoryReader, category, categoryReader.emptyCategoryPosition(category));
    }

    public SubscriptionBuilder<T> readingFrom(EventCategoryReader categoryReader, String category, Position startingPosition) {
        this.reader = pos -> categoryReader.readCategoryForwards(category, pos);
        this.readerDescription = EventSubscription.descriptionFor(categoryReader, category);
        this.startingPosition = startingPosition;
        return this;
    }

    public SubscriptionBuilder<T> publishingTo(Collection<EventHandler<T>> handlers) {
        this.handlers.addAll(handlers);
        return this;
    }

    public SubscriptionBuilder<T> publishingTo(EventHandler<? super T> handler) {
        this.handlers.add(handler);
        return this;
    }

    public SubscriptionBuilder<T> publishingTo(Consumer<? super T> handler) {
        this.handlers.add(EventHandler.ofConsumer(handler));
        return this;
    }

    public SubscriptionBuilder<T> withListeners(SubscriptionListener... listeners) {
        this.listeners.addAll(Arrays.asList(listeners));
        return this;
    }

    public SubscriptionBuilder<T> withListeners(Collection<SubscriptionListener> listeners) {
        this.listeners.addAll(listeners);
        return this;
    }

    public SubscriptionBuilder<T> withListener(SubscriptionListener listener) {
        this.listeners.add(listener);
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

        EventHandler<? super T> eventHandler;
        if (handlers.isEmpty()) {
            eventHandler = EventHandler.discard();
        }
        else if (handlers.size() == 1) {
            eventHandler = handlers.iterator().next();
        }
        else {
            eventHandler = new BroadcastingEventHandler<>(handlers);
        }

        return new EventSubscription<>(
                name,
                readerDescription,
                reader,
                deserializer,
                eventHandler,
                clock,
                bufferSize,
                runFrequency,
                startingPosition,
                initialReplay,
                staleness,
                listeners,
                eventSink
        );
    }

}
