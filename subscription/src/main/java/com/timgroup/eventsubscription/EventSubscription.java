package com.timgroup.eventsubscription;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventsubscription.healthcheck.ChaserHealth;
import com.timgroup.eventsubscription.healthcheck.EventSubscriptionStatus;
import com.timgroup.eventsubscription.healthcheck.SubscriptionListener;
import com.timgroup.eventsubscription.healthcheck.SubscriptionListenerAdapter;
import com.timgroup.structuredevents.EventSink;
import com.timgroup.structuredevents.Slf4jEventSink;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.Health;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.lmax.disruptor.dsl.ProducerType.SINGLE;
import static java.util.Collections.unmodifiableCollection;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class EventSubscription<T> {
    private static final Logger LOG = LoggerFactory.getLogger(EventSubscription.class);
    private final EventSubscriptionStatus subscriptionStatus;
    private final List<Component> statusComponents;
    private final ScheduledExecutorService chaserExecutor;
    private final ExecutorService eventHandlerExecutor;
    private final Disruptor<EventContainer<T>> disruptor;
    private final EventStoreChaser chaser;
    private final Duration runFrequency;

    public EventSubscription(
            String name,
            EventCategoryReader eventReader,
            String category,
            Deserializer<? extends T> deserializer,
            EventHandler<? super T> eventHandler,
            Clock clock,
            int bufferSize,
            Duration runFrequency,
            Position startingPosition,
            Duration maxInitialReplayDuration,
            List<SubscriptionListener> listeners
    ) {
        this(name, pos -> eventReader.readCategoryForwards(category, pos), deserializer, eventHandler, clock, bufferSize, runFrequency, startingPosition, maxInitialReplayDuration, listeners, new Slf4jEventSink());
    }

    public EventSubscription(
            String name,
            EventReader eventReader,
            Deserializer<? extends T> deserializer,
            EventHandler<? super T> eventHandler,
            Clock clock,
            int bufferSize,
            Duration runFrequency,
            Position startingPosition,
            Duration maxInitialReplayDuration,
            List<SubscriptionListener> listeners
    ) {
        this(name, eventReader::readAllForwards, deserializer, eventHandler, clock, bufferSize, runFrequency, startingPosition, maxInitialReplayDuration, listeners, new Slf4jEventSink());
    }

    EventSubscription(
                String name,
                Function<Position, Stream<ResolvedEvent>> eventSource,
                Deserializer<? extends T> deserializer,
                EventHandler<? super T> eventHandler,
                Clock clock,
                int bufferSize,
                Duration runFrequency,
                Position startingPosition,
                Duration maxInitialReplayDuration,
                List<SubscriptionListener> listeners,
                EventSink eventSink
    ) {
        this.runFrequency = runFrequency;
        ChaserHealth chaserHealth = new ChaserHealth(name, clock, runFrequency);
        subscriptionStatus = new EventSubscriptionStatus(name, clock, maxInitialReplayDuration, eventSink);

        List<SubscriptionListener> subListeners = new ArrayList<>();
        subListeners.add(subscriptionStatus);
        subListeners.addAll(listeners);
        SubscriptionListenerAdapter processorListener = new SubscriptionListenerAdapter(startingPosition, subListeners);

        chaserExecutor = Executors.newScheduledThreadPool(1, new NamedThreadFactory("EventChaser-" + name));
        eventHandlerExecutor = Executors.newCachedThreadPool(new NamedThreadFactory("EventSubscription-" + name));
        disruptor = new Disruptor<>(new EventContainer.Factory<>(), bufferSize, eventHandlerExecutor, SINGLE, new BlockingWaitStrategy());

        disruptor.handleExceptionsWith(new ExceptionHandler<EventContainer<T>>() {
            @Override
            public void handleEventException(Throwable ex, long sequence, EventContainer<T> event) {
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
                new DisruptorDeserializationAdapter<>(deserializer, processorListener),
                new DisruptorDeserializationAdapter<>(deserializer, processorListener)
        ).then(new DisruptorEventHandlerAdapter<>(eventHandler, processorListener));

        ChaserListener chaserListener = new BroadcastingChaserListener(chaserHealth, processorListener);
        EventContainer.Translator<T> translator = new EventContainer.Translator<>();
        chaser = new EventStoreChaser(eventSource, startingPosition, event -> disruptor.publishEvent(translator.setting(event)), chaserListener);

        statusComponents = new ArrayList<>();
        statusComponents.add(subscriptionStatus);
        statusComponents.add(chaserHealth);
    }

    public Health health() {
        return subscriptionStatus;
    }

    public Collection<Component> statusComponents() {
        return unmodifiableCollection(statusComponents);
    }

    public void start() {
        disruptor.start();
        chaserExecutor.scheduleWithFixedDelay(chaser, 0, runFrequency.toMillis(), MILLISECONDS);
    }

    public void stop() {
        try {
            chaserExecutor.shutdown();
            chaserExecutor.awaitTermination(1, TimeUnit.SECONDS);
            disruptor.halt();
            eventHandlerExecutor.shutdown();
            eventHandlerExecutor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
