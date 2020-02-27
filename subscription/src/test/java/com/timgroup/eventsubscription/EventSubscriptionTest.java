package com.timgroup.eventsubscription;

import com.timgroup.clocks.testing.ManualClock;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.memory.InMemoryEventSource;
import com.timgroup.structuredevents.testing.LocalEventSink;
import net.ttsui.junit.rules.pending.PendingImplementation;
import net.ttsui.junit.rules.pending.PendingRule;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

public class EventSubscriptionTest {
    @Rule public PendingRule pendingRule = new PendingRule();

    private final LocalEventSink eventSink = new LocalEventSink();

    @Test
    public void no_events_are_processed_after_calling_stop() throws InterruptedException {
        InMemoryEventSource eventSource = new InMemoryEventSource(new ManualClock(Instant.EPOCH, ZoneOffset.UTC));

        List<Event> processedEvents = new ArrayList<>();
        List<NewEvent> testEvents = IntStream.range(1, 1000).mapToObj(i -> NewEvent.newEvent("testEvent", String.valueOf(i).getBytes())).collect(toList());
        eventSource.writeStream().write(StreamId.streamId("all", "all"), testEvents);

        CountDownLatch latch = new CountDownLatch(1);

        AtomicReference<EventSubscription> subscriptionRef = new AtomicReference<>();
        Consumer<Event> eventHandler = (event) -> {
            processedEvents.add(event);
            String data = ((TestEvent) event).data;
            if (data.equals("500")) {
                subscriptionRef.get().stop();
                latch.countDown();
            }
        };
        subscriptionRef.set(SubscriptionBuilder.eventSubscription("all")
                .readingFrom(eventSource.readAll())
                .deserializingUsing(Deserializer.applying(eventRecord -> new TestEvent(new String(eventRecord.data()))))
                .publishingTo(eventHandler)
                .withEventSink(eventSink)
                .build());

        subscriptionRef.get().start();

        latch.await(5, TimeUnit.SECONDS);

        assertThat(processedEvents.size(), is(500));
    }

    @Test
    public void stopping_subscription_shuts_down_all_threads() throws InterruptedException {
        InMemoryEventSource eventSource = new InMemoryEventSource(new ManualClock(Instant.EPOCH, ZoneOffset.UTC));

        List<NewEvent> testEvents = IntStream.range(1, 6).mapToObj(i -> NewEvent.newEvent("testEvent", String.valueOf(i).getBytes())).collect(toList());
        eventSource.writeStream().write(StreamId.streamId("all", "all"), testEvents);

        CountDownLatch reachedShutdownPointLatch = new CountDownLatch(1);
        CountDownLatch eventProcessingStartedLatch = new CountDownLatch(1);

        int bufferSizeToEnsurePublishingBlocks = 2;

        AtomicReference<EventSubscription> subscriptionRef = new AtomicReference<>();
        Consumer<Event> eventHandler = (event) -> {
            eventProcessingStartedLatch.countDown();
            String data = ((TestEvent) event).data;
            if (data.equals(String.valueOf(bufferSizeToEnsurePublishingBlocks))) {
                subscriptionRef.get().stop();
                reachedShutdownPointLatch.countDown();
            }
        };
        subscriptionRef.set(SubscriptionBuilder.eventSubscription("all")
                .runningInParallelWithBuffer(bufferSizeToEnsurePublishingBlocks)
                .readingFrom(eventSource.readAll())
                .deserializingUsing(Deserializer.applying(eventRecord -> new TestEvent(new String(eventRecord.data()))))
                .publishingTo(eventHandler)
                .withEventSink(eventSink)
                .build());

        subscriptionRef.get().start();
        eventProcessingStartedLatch.await(5, TimeUnit.SECONDS);

        assertThat(eventSubscriptionThreads(), is(not(emptyIterable())));

        reachedShutdownPointLatch.await(5, TimeUnit.SECONDS);
        assertThat(eventSubscriptionThreads(), is(emptyIterable()));
    }

    private List<String> eventSubscriptionThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .map(Thread::getName)
                .filter(name -> name.startsWith("EventSubscription") || name.startsWith("EventChaser") )
                .collect(toList());
    }

    public static class TestEvent implements Event {
        public final String data;

        public TestEvent(String data) {
            this.data = data;
        }


        @Override
        public String toString() {
            return "TestEvent{" +
                    "data='" + data + '\'' +
                    '}';
        }
    }
}
