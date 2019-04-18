package com.timgroup.eventsubscription;

import com.timgroup.clocks.testing.ManualClock;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import com.timgroup.eventsubscription.healthcheck.DurationThreshold;
import com.timgroup.eventsubscription.healthcheck.InitialCatchupFuture;
import com.timgroup.eventsubscription.lifecycleevents.CaughtUp;
import com.timgroup.eventsubscription.lifecycleevents.InitialCatchupCompleted;
import com.timgroup.eventsubscription.lifecycleevents.SubscriptionLifecycleEvent;
import com.timgroup.eventsubscription.lifecycleevents.SubscriptionTerminated;
import com.timgroup.structuredevents.LocalEventSink;
import com.timgroup.structuredevents.StructuredEventMatcher;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.Report;
import com.youdevise.testutils.matchers.Contains;
import junit.framework.AssertionFailedError;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static com.timgroup.eventsubscription.SubscriptionBuilder.eventSubscription;
import static com.timgroup.tucker.info.Health.State.healthy;
import static com.timgroup.tucker.info.Health.State.ill;
import static com.timgroup.tucker.info.Status.CRITICAL;
import static com.timgroup.tucker.info.Status.OK;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.StringContains.containsString;

@SuppressWarnings("CodeBlock2Expr")
public class EndToEndTest {

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    private final ManualClock clock = ManualClock.createDefault();
    private final StreamId stream = streamId("any", "any");
    private final JavaInMemoryEventStore store = new JavaInMemoryEventStore(clock);

    private final LocalEventSink eventSink = new LocalEventSink();
    private final InitialCatchupFuture initialCatchupFuture = new InitialCatchupFuture();

    private EventSubscription subscription;

    @After
    public void stopSubscription() {
        if (subscription != null)
            subscription.stop();
    }

    @Test
    public void reports_ill_during_initial_replay() throws Exception {
        BlockingEventHandler eventProcessing = new BlockingEventHandler();
        store.write(stream, Arrays.asList(newEvent(), newEvent(), newEvent()));
        subscription = subscription(Deserializer.applying(EndToEndTest::deserialize), eventProcessing);
        subscription.start();

        eventually(() -> {
            assertThat(subscription.health().get(), is(ill));
            assertThat(statusComponent().getReport(), is(new Report(OK, "Awaiting initial catchup. No events processed yet. (Stale for PT0S)")));
        });

        eventProcessing.allowProcessing(1);
        eventually(() -> {
            assertThat(statusComponent().getReport(), is(new Report(OK, "Awaiting initial catchup. Currently at position 1. (Stale for PT0S)")));
        });

        clock.bumpSeconds(123);
        eventProcessing.continueProcessing();

        List<ResolvedEvent> allEventsInStore = store.readAllForwards().collect(Collectors.toList());

        eventually(() -> {
            assertThat(subscription.health().get(), is(healthy));
            assertThat(statusComponent().getReport(), is(new Report(OK, "Caught up at position 3. Initial replay took PT2M3S")));
            assertThat(eventSink.events(), Contains.only(StructuredEventMatcher.ofType("InitialReplayCompleted")));
        });
        assertThat(initialCatchupFuture.isDone(), equalTo(true));
    }

    @Test
    public void reports_warning_if_event_store_was_not_polled_recently() throws Exception {
        store.write(stream, Arrays.asList(newEvent(), newEvent(), newEvent()));
        subscription = subscription(Deserializer.applying(EndToEndTest::deserialize), failingHandler(() -> new RuntimeException("failure")));
        subscription.start();

        eventually(() -> {
            assertThat(statusComponent().getReport().getStatus(), is(CRITICAL));
            assertThat(statusComponent().getReport().getValue().toString(), containsString("Event subscription terminated. Failed to process position 1: failure"));
        });
    }

    @Test
    public void does_not_continue_processing_events_if_event_processing_failed_on_a_previous_event() throws Exception {
        List<Event> eventsProcessed = new CopyOnWriteArrayList<>();

        store.write(stream, Arrays.asList(newEvent(), newEvent()));
        subscription = subscription(Deserializer.applying(EndToEndTest::deserialize), EventHandler.ofConsumer(e -> {
            eventsProcessed.add(e);
            if (e instanceof DeserialisedEvent && ((DeserialisedEvent) e).event.eventNumber() == 0) {
                throw new RuntimeException("failure");
            }
        }));
        subscription.start();

        Thread.sleep(50L);

        eventually(() -> {
            assertThat(statusComponent().getReport().getStatus(), is(CRITICAL));
            assertThat(statusComponent().getReport().getValue().toString(), containsString("Event subscription terminated. Failed to process position 1: failure"));
            assertThat(eventsProcessed, Contains.inOrder(
                    equalTo(new DeserialisedEvent(store.readAllForwards().collect(Collectors.toList()).get(0).eventRecord())),
                    subscriptionTerminatedWithMessage("failure")
            ));
        });

        assertThat(initialCatchupFuture.isCompletedExceptionally(), equalTo(true));
    }

    @Test
    public void does_not_continue_processing_events_if_deserialisation_failed_on_a_previous_event() throws Exception {
        List<Event> eventsProcessed = new CopyOnWriteArrayList<>();
        store.write(stream, Arrays.asList(newEvent(), newEvent(), newEvent()));
        subscription = subscription(Deserializer.applying(e -> {
            if (e.eventNumber() == 0) {
                throw new RuntimeException("Failed to deserialize first event");
            } else {
                return deserialize(e);
            }
        }), EventHandler.ofConsumer(eventsProcessed::add));
        subscription.start();

        Thread.sleep(50L);

        eventually(() -> {
            assertThat(eventsProcessed, Contains.only(subscriptionTerminatedWithMessage("Failed to deserialize first event")));
        });
    }

    @Test
    public void invokes_event_handlers_with_position_and_deserialized_event() throws Exception {
        NewEvent event1 = newEvent();
        NewEvent event2 = newEvent();
        store.write(stream, Arrays.asList(event1, event2));
        List<EventWithPosition> receivedEvents = new CopyOnWriteArrayList<>();
        subscription = subscription(Deserializer.applying(EndToEndTest::deserialize), new EventHandler() {
            @Override
            public void apply(Position position, Event deserialized) {
                receivedEvents.add(new EventWithPosition(position, deserialized));
            }
        });
        subscription.start();

        eventually(() -> {
            assertThat(subscription.health().get(), is(healthy));
            assertThat(receivedEvents, hasSize(greaterThanOrEqualTo(4)));
        });

        List<ResolvedEvent> eventsInStore = store.readAllForwards().collect(Collectors.toList());

        assertThat(new ArrayList<>(receivedEvents).subList(0, 4), Contains.inOrder(
                new EventWithPosition(eventsInStore.get(0).position(), new DeserialisedEvent(eventRecord(clock.instant(), stream, 0, event1.type(), event1.data(), event1.metadata()))),
                new EventWithPosition(eventsInStore.get(1).position(), new DeserialisedEvent(eventRecord(clock.instant(), stream, 1, event2.type(), event2.data(), event2.metadata()))),
                new EventWithPosition(eventsInStore.get(1).position(), new InitialCatchupCompleted(eventsInStore.get(1).position(), clock.instant())),
                new EventWithPosition(eventsInStore.get(1).position(), new CaughtUp(eventsInStore.get(1).position(), clock.instant()))
        ));
    }

    @Test
    public void processes_only_category_events() throws Exception {
        List<Event> receivedEvents = new CopyOnWriteArrayList<>();

        NewEvent event1 = newEvent();
        NewEvent event2 = newEvent();
        NewEvent event3 = newEvent();
        store.write(streamId("alpha", "1"), singletonList(event1));
        store.write(streamId("beta", "1"), singletonList(event2));
        store.write(streamId("alpha", "2"), singletonList(event3));
        subscription = eventSubscription("test")
                .readingFrom(store, "alpha")
                .deserializingUsing(Deserializer.applying(EndToEndTest::deserialize))
                .publishingTo(EventHandler.ofConsumer(event -> {
                    if (event instanceof DeserialisedEvent) {
                        receivedEvents.add(event);
                    }
                }))
                .withClock(clock)
                .runningInParallelWithBuffer(1024)
                .withRunFrequency(Duration.ofMillis(1L))
                .build();
        subscription.start();

        eventually(() -> {
            assertThat(subscription.health().get(), is(healthy));
        });

        assertThat(receivedEvents, Contains.inOrder(
                new DeserialisedEvent(eventRecord(clock.instant(), streamId("alpha", "1"), 0, event1.type(), event1.data(), event1.metadata())),
                new DeserialisedEvent(eventRecord(clock.instant(), streamId("alpha", "2"), 0, event3.type(), event3.data(), event3.metadata()))
        ));
    }

    @Test
    public void starts_up_healthy_when_there_are_no_events() throws Exception {
        List<Event> eventsProcessed = new CopyOnWriteArrayList<>();
        subscription = subscription(Deserializer.applying(EndToEndTest::deserialize), EventHandler.ofConsumer(eventsProcessed::add));
        subscription.start();

        eventually(() -> {
            assertThat(subscription.health().get(), is(healthy));
            assertThat(eventsProcessed.get(0), instanceOf(InitialCatchupCompleted.class));
            assertThat(eventsProcessed.stream().anyMatch(e -> !(e instanceof SubscriptionLifecycleEvent)), is(false));
        });
    }

    @Test
    public void starts_up_healthy_when_there_are_no_events_after_fromversion() throws Exception {
        List<Event> eventsProcessed = new CopyOnWriteArrayList<>();

        store.write(stream, Arrays.asList(newEvent(), newEvent(), newEvent()));
        Position lastPosition = store.readAllForwards().map(ResolvedEvent::position).collect(last()).orElseThrow(() -> new AssertionError("No events"));
        subscription = subscription(Deserializer.applying(EndToEndTest::deserialize), EventHandler.ofConsumer(eventsProcessed::add), lastPosition);
        subscription.start();

        eventually(() -> {
            assertThat(subscription.health().get(), is(healthy));
            assertThat(eventsProcessed.get(0), instanceOf(InitialCatchupCompleted.class));
            assertThat(eventsProcessed.stream().anyMatch(e -> !(e instanceof SubscriptionLifecycleEvent)), is(false));
        });
    }

    @Test public void
    constructing_subscription_with_staleness_same_as_run_frequency_fails() {
        thrown.expectMessage("Staleness threshold is configured <= run frequency. This will result in a flickering alert.");

        SubscriptionBuilder.eventSubscription("test")
                .withRunFrequency(Duration.ofSeconds(1))
                .withStalenessThreshold(DurationThreshold.warningThresholdWithCriticalRatio(Duration.ofSeconds(1), 2))
                .readingFrom(store)
                .deserializingUsing((event, consumer) -> {})
                .build();
    }

    private EventSubscription subscription(Deserializer<DeserialisedEvent> deserializer, EventHandler eventHandler) {
        return subscription(deserializer, eventHandler, store.emptyStorePosition());
    }

    private EventSubscription subscription(Deserializer<DeserialisedEvent> deserializer, EventHandler eventHandler, Position startingPosition) {
        return eventSubscription("test")
                .withClock(clock)
                .withRunFrequency(Duration.ofMillis(1))
                .withMaxInitialReplayDuration(Duration.ofSeconds(320))
                .readingFrom(store, startingPosition)
                .deserializingUsing(deserializer)
                .publishingTo(eventHandler)
                .withEventSink(eventSink)
                .publishingTo(initialCatchupFuture)
                .build();
    }

    private static Matcher<Position> inMemoryPosition(long n) {
        return new TypeSafeDiagnosingMatcher<Position>() {
            @Override
            protected boolean matchesSafely(Position item, Description mismatchDescription) {
                mismatchDescription.appendText("position was ").appendValue(item);
                return Long.parseLong(item.toString()) == n;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("version ").appendValue(n);
            }
        };
    }

    private Component statusComponent() {
        return subscription.statusComponents().stream().filter(c -> c.getId().equals("event-subscription-status-test")).findFirst().orElseThrow(() -> new AssertionFailedError("No event-subscription-status-test component"));
    }

    private void eventually(Runnable work) throws Exception {
        Duration patience = Duration.ofSeconds(1);
        Duration interval = Duration.ofMillis(15);
        int remaining = (int) (patience.toMillis() / interval.toMillis());

        while (true) {
            try {
                work.run();
                return;
            } catch (Exception | AssertionError e) {
                if (remaining-- == 0) {
                    throw e;
                }
                Thread.sleep(interval.toMillis());
            }
        }
    }

    private static final class DeserialisedEvent implements Event {
        final EventRecord event;

        private DeserialisedEvent(EventRecord event) {
            this.event = event;
        }

        @Override
        public int hashCode() {
            return event.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof DeserialisedEvent && ((DeserialisedEvent) obj).event.equals(event);
        }

        @Override
        public String toString() {
            return event.toString();
        }
    }

    private static NewEvent newEvent() {
        return NewEvent.newEvent("A", "{}".getBytes(), "{}".getBytes());
    }

    private static DeserialisedEvent deserialize(EventRecord eventRecord) {
        return new DeserialisedEvent(eventRecord);
    }

    private static EventHandler failingHandler(Supplier<RuntimeException> supplier) {
        requireNonNull(supplier);
        return (position, event) -> { throw supplier.get(); };
    }

    private static final class BlockingEventHandler implements EventHandler {
        private final Semaphore lock = new Semaphore(0);

        @Override
        public void apply(Position position, Event deserialized) {
            try {
                lock.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        void allowProcessing(int count) {
            lock.release(count);
        }

        void continueProcessing() {
            lock.release(Integer.MAX_VALUE);
        }
    }

    private static <T> Collector<T, ?, Optional<T>> last() {
        return new Collector<T, ArrayList<T>, Optional<T>>() {
            @Override
            public Supplier<ArrayList<T>> supplier() {
                return () -> new ArrayList<>(1);
            }

            @Override
            public BiConsumer<ArrayList<T>, T> accumulator() {
                return (state, next) -> {
                    state.clear();
                    state.add(requireNonNull(next));
                };
            }

            @Override
            public BinaryOperator<ArrayList<T>> combiner() {
                return (l, r) -> {
                    if (l.isEmpty())
                        return r;
                    if (r.isEmpty())
                        return l;
                    throw new UnsupportedOperationException();
                };
            }

            @Override
            public Function<ArrayList<T>, Optional<T>> finisher() {
                return state -> state.isEmpty() ? Optional.empty() : Optional.of(state.get(0));
            }

            @Override
            public Set<Characteristics> characteristics() {
                return EnumSet.noneOf(Characteristics.class);
            }
        };
    }

    private static Matcher<? super Event> subscriptionTerminatedWithMessage(String msg) {
        return new TypeSafeMatcher<Event>() {
            @Override
            protected boolean matchesSafely(Event item) {
                if (item instanceof SubscriptionTerminated) {
                    SubscriptionTerminated subscriptionTerminated = (SubscriptionTerminated) item;
                    return subscriptionTerminated.exception.getMessage().contains(msg);
                }
                return false;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("SubscriptionTerminated with msg " + msg);
            }
        };
    }

    public static final class EventWithPosition {
        public final Position position;
        public final Event event;

        public EventWithPosition(Position position, Event event) {
            this.position = requireNonNull(position);
            this.event = requireNonNull(event);
        }

        @Override
        public String toString() {
            return "EventWithPosition{" +
                    "position=" + position +
                    ", event=" + event +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EventWithPosition that = (EventWithPosition) o;
            return Objects.equals(position, that.position) &&
                    Objects.equals(event, that.event);
        }

        @Override
        public int hashCode() {
            return Objects.hash(position, event);
        }
    }
}
