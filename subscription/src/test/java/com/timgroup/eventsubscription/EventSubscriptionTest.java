package com.timgroup.eventsubscription;

import com.timgroup.clocks.testing.ManualClock;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.memory.InMemoryEventSource;
import com.timgroup.structuredevents.testing.LocalEventSink;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class EventSubscriptionTest {

    private final LocalEventSink eventSink = new LocalEventSink();
    private final List<Event> events = new ArrayList<>();
    private final InMemoryEventSource eventSource = new InMemoryEventSource(new ManualClock(Instant.EPOCH, ZoneOffset.UTC));

    private EventSubscription subscription;

    @Test
    public void no_events_are_processed_after_calling_stop() throws InterruptedException {
        InMemoryEventSource eventSource = new InMemoryEventSource(new ManualClock(Instant.EPOCH, ZoneOffset.UTC));

        List<NewEvent> testEvents = IntStream.range(1, 1000).mapToObj(i -> NewEvent.newEvent("testEvent", String.valueOf(i).getBytes())).collect(Collectors.toList());
        eventSource.writeStream().write(StreamId.streamId("all", "all"), testEvents);

        CountDownLatch latch = new CountDownLatch(1);

        Consumer<Event> eventHandler = (event) -> {
            events.add(event);
            String data = ((TestEvent) event).data;
            if (data.equals("500")) {
                stopSubscription();
                latch.countDown();
            }
        };
        subscription = SubscriptionBuilder.eventSubscription("all")
                .readingFrom(eventSource.readAll())
                .deserializingUsing(Deserializer.applying(eventRecord -> new TestEvent(new String(eventRecord.data()))))
                .publishingTo(eventHandler)
                .withEventSink(eventSink)
                .build();

        subscription.start();

        latch.await(5, TimeUnit.SECONDS);

        assertThat(events.size(), is(500));
    }

    public void stopSubscription() {
        subscription.stop();
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
