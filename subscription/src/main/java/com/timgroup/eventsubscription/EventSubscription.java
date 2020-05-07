package com.timgroup.eventsubscription;

import com.codahale.metrics.MetricRegistry;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventsubscription.healthcheck.ChaserHealth;
import com.timgroup.eventsubscription.healthcheck.DurationThreshold;
import com.timgroup.eventsubscription.healthcheck.EventSubscriptionStatus;
import com.timgroup.eventsubscription.lifecycleevents.SubscriptionTerminated;
import com.timgroup.structuredevents.EventSink;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.Health;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.lmax.disruptor.dsl.ProducerType.SINGLE;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableCollection;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class EventSubscription {
    private static final Logger LOG = LoggerFactory.getLogger(EventSubscription.class);
    private final EventSubscriptionStatus subscriptionStatus;
    private final List<Component> statusComponents;
    private final ScheduledExecutorService chaserExecutor;
    private final ExecutorService eventHandlerExecutor;
    private final Disruptor<EventContainer> disruptor;
    private final EventStoreChaser chaser;
    private final Duration runFrequency;
    private final AtomicBoolean running = new AtomicBoolean(false);

    EventSubscription(
                String name,
                String description,
                Function<Position, Stream<ResolvedEvent>> eventSource,
                Deserializer<? extends Event> deserializer,
                EventHandler eventHandler,
                Clock clock,
                int bufferSize,
                Duration runFrequency,
                Position startingPosition,
                DurationThreshold initialReplay,
                DurationThreshold staleness,
                EventSink eventSink,
                Optional<MetricRegistry> metricRegistry
    ) {
        this.runFrequency = runFrequency;
        ChaserHealth chaserHealth = new ChaserHealth(name, clock, runFrequency);
        subscriptionStatus = new EventSubscriptionStatus(name, clock, initialReplay, staleness, eventSink);

        chaserExecutor = Executors.newScheduledThreadPool(1, new NamedThreadFactory("EventChaser-" + name));
        eventHandlerExecutor = Executors.newCachedThreadPool(new NamedThreadFactory("EventSubscription-" + name));
        disruptor = new Disruptor<>(new EventContainer.Factory(), bufferSize, eventHandlerExecutor, SINGLE, new BlockingWaitStrategy());

        disruptor.handleExceptionsWith(new ExceptionHandler<EventContainer>() {
            @Override
            public void handleEventException(Throwable ex, long sequence, EventContainer event) {
                LOG.error(String.format("Exception processing %d: %s", sequence, event), ex);
                if (ex instanceof RuntimeException) {
                    throw (RuntimeException) ex;
                } else if (ex instanceof Error) {
                    throw (Error) ex;
                } else {
                    throw new RuntimeException(String.format("Exception processing %d: %s", sequence, event), ex);
                }
            }

            @Override
            public void handleOnStartException(Throwable ex) {
                LOG.error("Error starting up disruptor", ex);
            }

            @Override
            public void handleOnShutdownException(Throwable ex) {
                LOG.error("Error shutting down disruptor", ex);
            }
        });
        disruptor.handleEventsWithWorkerPool(
                new DisruptorDeserializationAdapter(deserializer),
                new DisruptorDeserializationAdapter(deserializer)
        ).then(new DisruptorEventHandlerAdapter((position, deserialized) -> {
            if (running.get()) {
                boolean applied = false;
                try {
                    eventHandler.apply(position, deserialized);
                    applied = true;
                } finally {
                    if (applied || (deserialized instanceof SubscriptionTerminated)) {
                        subscriptionStatus.apply(position, deserialized);
                    }
                }
            }
        }, metricRegistry.map(r -> r.counter(String.format("eventsubscription.%s.missedCatchup", name)))));

        chaser = new EventStoreChaser(eventSource, startingPosition, disruptor, chaserHealth, clock, metricRegistry.map(r -> r.counter(format("eventsubscription.%s.chaserRun", name))), running::get);

        statusComponents = new ArrayList<>();
        statusComponents.add(Component.supplyInfo("event-subscription-description", "Subscription source (" + name + ")", () -> description));
        statusComponents.add(subscriptionStatus);
        statusComponents.add(chaserHealth);

        metricRegistry.ifPresent(registry -> {
            registry.gauge(String.format("eventsubscription.%s.bufferSize", name), () -> () -> bufferSize);
            registry.gauge(String.format("eventsubscription.%s.bufferFree", name), () -> () -> disruptor.getRingBuffer().remainingCapacity());
        });
    }

    public Health health() {
        return subscriptionStatus;
    }

    public Collection<Component> statusComponents() {
        return unmodifiableCollection(statusComponents);
    }

    public void start() {
        running.set(true);
        subscriptionStatus.notifyStarted();
        disruptor.start();
        chaserExecutor.scheduleWithFixedDelay(chaser, 0, runFrequency.toMillis(), MILLISECONDS);
    }

    public void stop() {
        try {
            running.set(false);
            chaserExecutor.shutdown();
            disruptor.halt();
            eventHandlerExecutor.shutdown();
            chaserExecutor.awaitTermination(1, TimeUnit.SECONDS);
            eventHandlerExecutor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static String descriptionFor(EventCategoryReader eventReader,
                                  String category) {
        return "reader=" + eventReader.toString() + ",category="+category;
    }

    public static String descriptionFor(EventReader eventReader) {
        return eventReader.toString();
    }
}
