package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.Report;
import com.timgroup.tucker.info.Status;

import java.util.Optional;
import java.util.function.Supplier;

import static java.lang.String.format;

/**
 * Reports whether the application can connect to a given event source.
 */
public final class EventStoreConnectionComponent extends Component {
    private final EventSource eventSource;
    private final Supplier<String> eventSourceMetadataSupplier;

    public EventStoreConnectionComponent(String id, String label, EventSource eventSource) {
        super(id, label);
        this.eventSource = eventSource;
        this.eventSourceMetadataSupplier = () -> "none supplied";
    }

    public EventStoreConnectionComponent(String id, String label, EventSource eventSource, Supplier<String> eventSourceMetadataSupplier) {
        super(id, label);
        this.eventSource = eventSource;
        this.eventSourceMetadataSupplier = eventSourceMetadataSupplier;
    }

    @Override
    public Report getReport() {
        try {
            long before = System.currentTimeMillis();
            Optional<ResolvedEvent> maybeLastEvent = eventSource.readAll().readLastEvent();
            long after = System.currentTimeMillis();
            String durationText = (after - before) + "ms";

            return new Report(Status.OK, maybeLastEvent
                    .map(event -> format("last event read in %s: %s%n%s", durationText, event.locator(), metadataText()))
                    .orElse(format("empty event store read in %s%n%s", durationText, metadataText())));
        } catch (RuntimeException e) {
            return new Report(Status.CRITICAL, format("%s%n%s", e.getMessage(), metadataText()));
        }
    }

    private String metadataText() {
        return format("EventStore metadata: %s", eventSourceMetadataSupplier.get());
    }
}
