package com.timgroup.eventsubscription;

import com.timgroup.clocks.testing.ManualClock;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import com.timgroup.structuredevents.LocalEventSink;
import com.timgroup.structuredevents.StructuredEventMatcher;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.Report;
import com.youdevise.testutils.matchers.Contains;
import junit.framework.AssertionFailedError;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static com.timgroup.tucker.info.Health.State.healthy;
import static com.timgroup.tucker.info.Health.State.ill;
import static com.timgroup.tucker.info.Status.CRITICAL;
import static com.timgroup.tucker.info.Status.OK;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.StringContains.containsString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class EndToEndTest {
    private final ManualClock clock = ManualClock.createDefault();
    private final StreamId stream = streamId("any", "any");
    private final JavaInMemoryEventStore store = new JavaInMemoryEventStore(clock);

    private final LocalEventSink eventSink = new LocalEventSink();
    private final Deserializer<DeserialisedEvent> deserializer = event -> deserialize(event);

    private EventSubscription<DeserialisedEvent> subscription;

    @After
    public void stopSubscription() {
        if (subscription != null)
            subscription.stop();
    }

    @Test
    public void reports_ill_during_initial_replay() throws Exception {
        BlockingEventHandler eventProcessing = new BlockingEventHandler();
        store.write(stream, Arrays.asList(newEvent(), newEvent(), newEvent()));
        subscription = subscription(deserializer, eventProcessing);
        subscription.start();

        eventually(() -> {
            assertThat(subscription.health().get(), is(ill));
            assertThat(statusComponent().getReport(), is(new Report(OK, "Stale, catching up. No events processed yet. (Stale for PT0S)")));
        });

        eventProcessing.allowProcessing(1);
        eventually(() -> {
            assertThat(statusComponent().getReport(), is(new Report(OK, "Stale, catching up. Currently at version 1. (Stale for PT0S)")));
        });

        clock.bumpSeconds(123);
        eventProcessing.allowProcessing(2);

        eventually(() -> {
            assertThat(subscription.health().get(), is(healthy));
            assertThat(statusComponent().getReport(), is(new Report(OK, "Caught up at version 3. Initial replay took PT2M3S")));
            assertThat(eventSink.events(), Contains.only(StructuredEventMatcher.ofType("InitialReplayCompleted")));
        });
    }

    @Test
    public void reports_warning_if_event_store_was_not_polled_recently() throws Exception {
        store.write(stream, Arrays.asList(newEvent(), newEvent(), newEvent()));
        subscription = subscription(deserializer, failingHandler(() -> new RuntimeException("failure")));
        subscription.start();

        eventually(() -> {
            assertThat(statusComponent().getReport().getStatus(), is(CRITICAL));
            assertThat(statusComponent().getReport().getValue().toString(), containsString("Event subscription terminated. Failed to process version 1: failure"));
        });
    }

    @Test
    public void does_not_continue_processing_events_if_event_processing_failed_on_a_previous_event() throws Exception {
        store.write(stream, Arrays.asList(newEvent(), newEvent()));
        AtomicInteger eventsProcessed = new AtomicInteger();
        subscription = subscription(deserializer, handler(e -> {
            eventsProcessed.incrementAndGet();
            if (e.event.eventNumber() == 0) {
                throw new RuntimeException("failure");
            }
        }));
        subscription.start();

        Thread.sleep(50L);

        eventually(() -> {
            assertThat(statusComponent().getReport().getStatus(), is(CRITICAL));
            assertThat(statusComponent().getReport().getValue().toString(), containsString("Event subscription terminated. Failed to process version 1: failure"));
            assertThat(eventsProcessed.get(), is(1));
        });
    }

    @Test
    public void does_not_continue_processing_events_if_deserialisation_failed_on_a_previous_event() throws Exception {
        store.write(stream, Arrays.asList(newEvent(), newEvent(), newEvent()));
        AtomicInteger eventsProcessed = new AtomicInteger();
        subscription = subscription(e -> {
            if (e.eventNumber() == 0) {
                throw new RuntimeException("Failed to deserialize first event");
            }
            else {
                return deserialize(e);
            }
        }, handler(e -> {
            eventsProcessed.incrementAndGet();
            throw new UnsupportedOperationException();
        }));
        subscription.start();

        Thread.sleep(50L);

        eventually(() -> {
            assertThat(eventsProcessed.get(), is(0));
        });
    }

    @Test
    public void invokes_event_handlers_with_both_event_record_and_deserialised_event() throws Exception {
        NewEvent event1 = newEvent();
        NewEvent event2 = newEvent();
        store.write(stream, Arrays.asList(event1, event2));
        @SuppressWarnings("unchecked")
        EventHandler<DeserialisedEvent> eventHandler = Mockito.mock(EventHandler.class);
        subscription = subscription(deserializer, eventHandler);
        subscription.start();

        eventually(() -> {
            assertThat(subscription.health().get(), is(healthy));
        });

        verify(eventHandler).apply(
                Matchers.argThat(inMemoryPosition(1)),
                Matchers.eq(new DateTime(clock.millis(), DateTimeZone.getDefault())),
                Matchers.eq(new DeserialisedEvent(eventRecord(clock.instant(), stream, 0, event1.type(), event1.data(), event1.metadata()))),
                Matchers.anyBoolean());

        verify(eventHandler).apply(
                Matchers.argThat(inMemoryPosition(2)),
                Matchers.eq(new DateTime(clock.millis(), DateTimeZone.getDefault())),
                Matchers.eq(new DeserialisedEvent(eventRecord(clock.instant(), stream, 1, event2.type(), event2.data(), event2.metadata()))),
                Matchers.eq(true));
    }

    @Test
    public void processes_only_category_events() throws Exception {
        NewEvent event1 = newEvent();
        NewEvent event2 = newEvent();
        NewEvent event3 = newEvent();
        store.write(streamId("alpha", "1"), Arrays.asList(event1));
        store.write(streamId("beta", "1"), Arrays.asList(event2));
        store.write(streamId("alpha", "2"), Arrays.asList(event3));
        @SuppressWarnings("unchecked")
        EventHandler<DeserialisedEvent> eventHandler = Mockito.mock(EventHandler.class);
        subscription = new EventSubscription<>("test", store, "alpha", EndToEndTest::deserialize, singletonList(eventHandler), clock, 1024, ofMillis(1L), store.emptyStorePosition(), ofSeconds(320), emptyList());
        subscription.start();

        eventually(() -> {
            assertThat(subscription.health().get(), is(healthy));
        });

        verify(eventHandler).apply(
                Matchers.argThat(inMemoryPosition(1)),
                Matchers.eq(new DateTime(clock.millis(), DateTimeZone.getDefault())),
                Matchers.eq(new DeserialisedEvent(eventRecord(clock.instant(), streamId("alpha", "1"), 0, event1.type(), event1.data(), event1.metadata()))),
                Matchers.anyBoolean());

        verify(eventHandler).apply(
                Matchers.argThat(inMemoryPosition(3)),
                Matchers.eq(new DateTime(clock.millis(), DateTimeZone.getDefault())),
                Matchers.eq(new DeserialisedEvent(eventRecord(clock.instant(), streamId("alpha", "2"), 0, event3.type(), event3.data(), event3.metadata()))),
                Matchers.eq(true));

        verifyNoMoreInteractions(eventHandler);
    }

    @Test
    public void starts_up_healthy_when_there_are_no_events() throws Exception {
        AtomicInteger eventsProcessed = new AtomicInteger();
        subscription = subscription(deserializer, handler(e -> {eventsProcessed.incrementAndGet(); }));
        subscription.start();

        eventually(() -> {
            assertThat(subscription.health().get(), is(healthy));
            assertThat(eventsProcessed.get(), is(0));
        });
    }

    @Test
    public void starts_up_healthy_when_there_are_no_events_after_fromversion() throws Exception {
        store.write(stream, Arrays.asList(newEvent(), newEvent(), newEvent()));
        Position lastPosition = store.readAllForwards().map(ResolvedEvent::position).collect(last()).get();
        AtomicInteger eventsProcessed = new AtomicInteger();
        subscription = subscription(deserializer, handler(e -> eventsProcessed.incrementAndGet()), lastPosition);
        subscription.start();

        eventually(() -> {
            assertThat(subscription.health().get(), is(healthy));
            assertThat(eventsProcessed.get(), is(0));
        });
    }

    private EventSubscription<DeserialisedEvent> subscription(Deserializer<DeserialisedEvent> deserializer, EventHandler<DeserialisedEvent> eventHandler) {
        return subscription(deserializer, eventHandler, store.emptyStorePosition());
    }

    private EventSubscription<DeserialisedEvent> subscription(Deserializer<DeserialisedEvent> deserializer, EventHandler<DeserialisedEvent> eventHandler, Position startingPosition) {
        return SubscriptionBuilder.<DeserialisedEvent>eventSubscription("test")
                .withClock(clock)
                .withRunFrequency(ofMillis(1))
                .withMaxInitialReplayDuration(ofSeconds(320))
                .readingFrom(store, startingPosition)
                .deserializingUsing(deserializer)
                .publishingTo(eventHandler)
                .withEventSink(eventSink)
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
        int remaining = 10;
        Duration interval = ofMillis(15);

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

    private static final class DeserialisedEvent {
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

    private static EventHandler<DeserialisedEvent> handler(Consumer<DeserialisedEvent> consumer) {
        return new EventHandler<DeserialisedEvent>() {
            @Override
            public void apply(DeserialisedEvent deserialized) {
                consumer.accept(deserialized);
            }
        };
    };

    private static EventHandler<DeserialisedEvent> failingHandler(Supplier<RuntimeException> supplier) {
        return handler(e -> { throw supplier.get(); });
    }

    private static final class BlockingEventHandler implements EventHandler<DeserialisedEvent> {
        private final Semaphore lock = new Semaphore(0);

        @Override
        public void apply(DeserialisedEvent deserialized) {
            try {
                lock.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        public void allowProcessing(int count) {
            lock.release(count);
        }
    }

    private static <T> Collector<T, ?, Optional<T>> last() {
        return new Collector<T, ArrayList<T>, Optional<T>>() {
            @Override
            public Supplier<ArrayList<T>> supplier() {
                return () -> new ArrayList<T>(1);
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
}
