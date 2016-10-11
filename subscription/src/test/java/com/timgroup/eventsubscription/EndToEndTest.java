package com.timgroup.eventsubscription;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.timgroup.clocks.testing.ManualClock;
import com.timgroup.eventstore.api.EventData;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.LegacyPositionAdapter;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.memory.InMemoryEventStore;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.Report;
import junit.framework.AssertionFailedError;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static com.timgroup.tucker.info.Health.State.healthy;
import static com.timgroup.tucker.info.Health.State.ill;
import static com.timgroup.tucker.info.Status.CRITICAL;
import static com.timgroup.tucker.info.Status.OK;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.StringContains.containsString;
import static org.joda.time.DateTimeZone.UTC;
import static org.mockito.Mockito.verify;
import static scala.collection.JavaConversions.asScalaBuffer;

public class EndToEndTest {
    private final ManualClock clock = ManualClock.createDefault();
    private final StreamId stream = streamId("any", "any");

    private EventSubscription<DeserialisedEvent> subscription;

    @After
    public void stopSubscription() {
        if (subscription != null)
            subscription.stop();
    }

    @Test
    public void reports_ill_during_initial_replay() throws Exception {
        InMemoryEventStore store = new InMemoryEventStore(() -> new DateTime(clock.millis(), UTC));
        BlockingEventHandler eventProcessing = new BlockingEventHandler();
        store.save(asScalaBuffer(Arrays.asList(anEvent(), anEvent(), anEvent())), scala.Option.empty());
        subscription = new EventSubscription<>("test", new LegacyEventStoreEventReaderAdapter(store), EndToEndTest::deserialize, singletonList(eventProcessing), clock, 1024, 1L, new LegacyPositionAdapter(0L), 320, emptyList());
        subscription.start();

        eventually(() -> {
            assertThat(subscription.health().get(), is(ill));
            assertThat(statusComponent().getReport(), is(new Report(OK, "Stale, catching up. No events processed yet. (Stale for 0s)")));
        });

        eventProcessing.allowProcessing(1);
        eventually(() -> {
            assertThat(statusComponent().getReport(), is(new Report(OK, "Stale, catching up. Currently at version 1. (Stale for 0s)")));
        });

        clock.bumpSeconds(123);
        eventProcessing.allowProcessing(2);

        eventually(() -> {
            assertThat(subscription.health().get(), is(healthy));
            assertThat(statusComponent().getReport(), is(new Report(OK, "Caught up at version 3. Initial replay took 123s.")));
        });
    }

    @Test
    public void reports_warning_if_event_store_was_not_polled_recently() throws Exception {
        InMemoryEventStore store = new InMemoryEventStore(() -> new DateTime(clock.millis(), UTC));
        store.save(asScalaBuffer(Arrays.asList(anEvent(), anEvent(), anEvent())), scala.Option.empty());
        subscription = new EventSubscription<>("test", new LegacyEventStoreEventReaderAdapter(store), EndToEndTest::deserialize, singletonList(failingHandler(() -> new RuntimeException("failure"))), clock, 1024, 1L, new LegacyPositionAdapter(0L), 320, emptyList());
        subscription.start();

        eventually(() -> {
            assertThat(statusComponent().getReport().getStatus(), is(CRITICAL));
            assertThat(statusComponent().getReport().getValue().toString(), containsString("Event subscription terminated. Failed to process version 1: failure"));
        });
    }

    @Test
    public void does_not_continue_processing_events_if_event_processing_failed_on_a_previous_event() throws Exception {
        InMemoryEventStore store = new InMemoryEventStore(() -> new DateTime(clock.millis(), UTC));
        store.save(asScalaBuffer(Arrays.asList(anEvent(), anEvent())), scala.Option.empty());
        AtomicInteger eventsProcessed = new AtomicInteger();
        subscription = new EventSubscription<>("test", new LegacyEventStoreEventReaderAdapter(store), EndToEndTest::deserialize, singletonList(handler(e -> {
            eventsProcessed.incrementAndGet();
            if (e.event.eventNumber() == 1) {
                throw new RuntimeException("failure");
            }
        })), clock, 1024, 1L, new LegacyPositionAdapter(0L), 320, emptyList());
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
        InMemoryEventStore store = new InMemoryEventStore(() -> new DateTime(clock.millis(), UTC));
        store.save(asScalaBuffer(Arrays.asList(anEvent(), anEvent(), anEvent())), scala.Option.empty());
        AtomicInteger eventsProcessed = new AtomicInteger();
        subscription = new EventSubscription<>("test", new LegacyEventStoreEventReaderAdapter(store), e -> {
            if (e.eventNumber() == 1) {
                throw new RuntimeException("Failed to deserialize event 1");
            }
            else {
                return deserialize(e);
            }
        }, singletonList(handler(e -> {
            eventsProcessed.incrementAndGet();
            throw new UnsupportedOperationException();
        })), clock, 1024, 1L, new LegacyPositionAdapter(0L), 320, emptyList());
        subscription.start();

        Thread.sleep(50L);

        eventually(() -> {
            assertThat(eventsProcessed.get(), is(0));
        });
    }

    @Test
    public void invokes_event_handlers_with_both_event_record_and_deserialised_event() throws Exception {
        InMemoryEventStore store = new InMemoryEventStore(() -> new DateTime(clock.millis(), UTC));
        EventData event1 = anEvent();
        EventData event2 = anEvent();
        store.save(asScalaBuffer(Arrays.asList(event1, event2)), scala.Option.empty());
        @SuppressWarnings("unchecked")
        EventHandler<DeserialisedEvent> eventHandler = Mockito.mock(EventHandler.class);
        subscription = new EventSubscription<>("test", new LegacyEventStoreEventReaderAdapter(store), EndToEndTest::deserialize, singletonList(eventHandler), clock, 1024, 1L, new LegacyPositionAdapter(0L), 320, emptyList());
        subscription.start();

        eventually(() -> {
            assertThat(subscription.health().get(), is(healthy));
        });

        verify(eventHandler).apply(
                Matchers.eq(new LegacyPositionAdapter(1)),
                Matchers.eq(new DateTime(clock.millis(), DateTimeZone.getDefault())),
                Matchers.eq(new DeserialisedEvent(eventRecord(clock.instant(), StreamId.streamId("all", "all"), 1, event1.eventType(), event1.body().data(), new byte[0]))),
                Matchers.anyBoolean());

        verify(eventHandler).apply(
                Matchers.eq(new LegacyPositionAdapter(2)),
                Matchers.eq(new DateTime(clock.millis(), DateTimeZone.getDefault())),
                Matchers.eq(new DeserialisedEvent(eventRecord(clock.instant(), StreamId.streamId("all", "all"), 2, event2.eventType(), event2.body().data(), new byte[0]))),
                Matchers.eq(true));
    }

    @Test
    public void starts_up_healthy_when_there_are_no_events() throws Exception {
        InMemoryEventStore store = new InMemoryEventStore(() -> new DateTime(clock.millis(), UTC));
        AtomicInteger eventsProcessed = new AtomicInteger();
        subscription = new EventSubscription<>("test", new LegacyEventStoreEventReaderAdapter(store), EndToEndTest::deserialize, singletonList(handler(e -> {
            eventsProcessed.incrementAndGet();
        })), clock, 1024, 1L, new LegacyPositionAdapter(0L), 320, emptyList());
        subscription.start();

        eventually(() -> {
            assertThat(subscription.health().get(), is(healthy));
            assertThat(eventsProcessed.get(), is(0));
        });
    }

    @Test
    public void starts_up_healthy_when_there_are_no_events_after_fromversion() throws Exception {
        InMemoryEventStore store = new InMemoryEventStore(() -> new DateTime(clock.millis(), UTC));
        store.save(asScalaBuffer(Arrays.asList(anEvent(), anEvent(), anEvent())), scala.Option.empty());
        AtomicInteger eventsProcessed = new AtomicInteger();
        subscription = new EventSubscription<>("test", new LegacyEventStoreEventReaderAdapter(store), EndToEndTest::deserialize, singletonList(handler(e -> {
            eventsProcessed.incrementAndGet();
        })), clock, 1024, 1L, new LegacyPositionAdapter(3L), 320, emptyList());
        subscription.start();

        eventually(() -> {
            assertThat(subscription.health().get(), is(healthy));
            assertThat(eventsProcessed.get(), is(0));
        });
    }

    private Component statusComponent() {
        return subscription.statusComponents().stream().filter(c -> c.getId().equals("event-subscription-status-test")).findFirst().orElseThrow(() -> new AssertionFailedError("No event-subscription-status-test component"));
    }

    private void eventually(Runnable work) throws Exception {
        int remaining = 10;
        Duration interval = Duration.ofMillis(15);

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

    private static EventData anEvent() {
        return EventData.apply("A", "{}".getBytes());
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
}
