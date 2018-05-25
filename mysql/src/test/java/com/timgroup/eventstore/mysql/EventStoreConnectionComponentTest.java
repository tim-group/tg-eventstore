package com.timgroup.eventstore.mysql;


import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.memory.InMemoryEventSource;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import com.timgroup.tucker.info.Report;
import com.timgroup.tucker.info.Status;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.time.Clock;

import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class EventStoreConnectionComponentTest {

    private final JavaInMemoryEventStore eventStore = new JavaInMemoryEventStore(Clock.systemUTC());
    private final InMemoryEventSource eventSource = new InMemoryEventSource(eventStore);

    @Test public void
    reports_OK_when_empty_event_store_is_successfully_read() {
        EventStoreConnectionComponent component = new EventStoreConnectionComponent("id", "label", eventSource);

        Report report = component.getReport();

        assertThat(report.getStatus(), is(Status.OK));
        assertThat(report.getValue().toString(), containsString("empty event store read"));
    }

    @Test public void
    reports_OK_and_last_event_locator_when_non_empty_event_store_is_successfully_read() {
        EventStoreConnectionComponent component = new EventStoreConnectionComponent("id", "label", eventSource);
        eventSource.writeStream().write(
                streamId("category", "id"),
                asList(
                        newEvent("FirstEvent", new byte[0], new byte[0]),
                        newEvent("SecondEvent", new byte[0], new byte[0])
                )
        );

        Report report = component.getReport();

        assertThat(report.getStatus(), is(Status.OK));
        assertThat(report.getValue().toString(), containsString("@2<category/id/1>(SecondEvent)"));
    }

    @Test public void
    reports_CRITICAL_when_event_store_cannot_be_read() {
        EventStoreConnectionComponent component = new EventStoreConnectionComponent("id", "label", new FailingEventSource(eventStore));

        Report report = component.getReport();

        assertThat(report.getStatus(), is(Status.CRITICAL));
        assertThat(report.getValue().toString(), containsString("failure when reading events"));
    }


    private static final class FailingEventSource extends InMemoryEventSource {
        public FailingEventSource(JavaInMemoryEventStore eventStore) { super(eventStore);}

        @Override @Nonnull public EventReader readAll() {
            throw new RuntimeException("failure when reading events");
        }
    }
}