package com.timgroup.eventsubscription;

import com.codahale.metrics.MetricRegistry;
import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventsubscription.healthcheck.DurationThreshold;
import com.timgroup.structuredevents.EventSink;
import com.timgroup.structuredevents.Slf4jEventSink;

import javax.annotation.Nonnull;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class SubscriptionBuilder {
    private final String name;
    private Clock clock = Clock.systemUTC();
    private Duration runFrequency = Duration.ofSeconds(1);
    private DurationThreshold initialReplay = new DurationThreshold(Duration.ofSeconds(1), Duration.ofSeconds(2));
    private DurationThreshold staleness;
    private int bufferSize = 1024;
    private final List<EventHandler> handlers = new ArrayList<>();

    private Function<Position, Stream<ResolvedEvent>> reader = null;
    private SubscriptionCanceller canceller = null;
    private Position startingPosition = null;
    private Deserializer<? extends Event> deserializer = null;
    private EventSink eventSink = new Slf4jEventSink();
    private String readerDescription = null;
    private MetricRegistry metricRegistry = null;

    private SubscriptionBuilder(String name) {
        this.name = name;
    }

    public static  SubscriptionBuilder eventSubscription(String name) {
        return new SubscriptionBuilder(name);
    }

    public SubscriptionBuilder deserializingUsing(Deserializer<? extends Event> deserializer) {
        this.deserializer = requireNonNull(deserializer);
        return this;
    }

    public SubscriptionBuilder withClock(Clock clock) {
        this.clock = requireNonNull(clock);
        return this;
    }

    public SubscriptionBuilder runningInParallelWithBuffer(int bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }

    public SubscriptionBuilder withRunFrequency(Duration runFrequency) {
        this.runFrequency = requireNonNull(runFrequency);
        return this;
    }

    public SubscriptionBuilder withMetrics(MetricRegistry metricRegistry) {
        this.metricRegistry = requireNonNull(metricRegistry);
        return this;
    }

    public SubscriptionBuilder withMaxInitialReplayDuration(Duration maxInitialReplayDuration) {
        this.initialReplay = DurationThreshold.warningThresholdWithCriticalRatio(maxInitialReplayDuration, 1.25);
        return this;
    }

    public SubscriptionBuilder withMaxInitialReplayDuration(DurationThreshold initialReplay) {
        this.initialReplay = requireNonNull(initialReplay);
        return this;
    }

    public SubscriptionBuilder withStalenessThreshold(DurationThreshold threshold) {
        this.staleness = requireNonNull(threshold);
        return this;
    }

    public SubscriptionBuilder readingFrom(EventReader eventReader) {
        return readingFrom(eventReader, eventReader.emptyStorePosition());
    }

    public SubscriptionBuilder readingFrom(EventReader eventReader, Position startingPosition) {
        this.reader = eventReader::readAllForwards;
        this.readerDescription = EventSubscription.descriptionFor(eventReader);
        this.startingPosition = startingPosition;
        return this;
    }

    public SubscriptionBuilder readingFrom(EventCategoryReader categoryReader, String category) {
        return readingFrom(categoryReader, category, categoryReader.emptyCategoryPosition(category));
    }

    public SubscriptionBuilder readingFrom(EventCategoryReader categoryReader, String category, Position startingPosition) {
        this.reader = pos -> categoryReader.readCategoryForwards(category, pos);
        this.readerDescription = EventSubscription.descriptionFor(categoryReader, category);
        this.startingPosition = startingPosition;
        return this;
    }

    public SubscriptionBuilder publishingTo(Collection<? extends EventHandler> handlers) {
        handlers.forEach(this::publishingTo);
        return this;
    }

    public SubscriptionBuilder publishingTo(EventHandler handler) {
        this.handlers.add(requireNonNull(handler));
        return this;
    }

    public SubscriptionBuilder publishingTo(Consumer<? super Event> handler) {
        this.handlers.add(EventHandler.ofConsumer(handler));
        return this;
    }

    public SubscriptionBuilder cancellingWhen(SubscriptionCanceller canceller) {
        this.canceller = canceller;
        return this;
    }

    public SubscriptionBuilder withEventSink(EventSink eventSink) {
        this.eventSink = requireNonNull(eventSink);
        return this;
    }

    @Nonnull
    public EventSubscription build() {
        requireNonNull(reader);
        requireNonNull(startingPosition);
        requireNonNull(deserializer);

        EventHandler eventHandler;
        if (handlers.isEmpty()) {
            eventHandler = EventHandler.DISCARD;
        }
        else if (handlers.size() == 1) {
            eventHandler = handlers.iterator().next();
        }
        else {
            eventHandler = new SequencingEventHandler(handlers);
        }

        if (staleness == null) {
            staleness = new DurationThreshold(runFrequency.multipliedBy(5), runFrequency.multipliedBy(30));
        }

        if (staleness.getWarning().compareTo(runFrequency) <= 0) {
            throw new RuntimeException("Staleness threshold is configured <= run frequency. This will result in a flickering alert.");
        }

        return new EventSubscription(
                name,
                readerDescription,
                reader,
                canceller,
                deserializer,
                eventHandler,
                clock,
                bufferSize,
                runFrequency,
                startingPosition,
                initialReplay,
                staleness,
                eventSink,
                Optional.ofNullable(metricRegistry)
        );
    }

}
