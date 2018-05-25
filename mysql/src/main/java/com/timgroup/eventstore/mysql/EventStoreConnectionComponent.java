package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.Report;
import com.timgroup.tucker.info.Status;

import java.util.Optional;

import static java.lang.String.format;

/**
 * Reports whether the application can connect to a given event source.
 */
public final class EventStoreConnectionComponent extends Component {
    private final EventSource eventSource;

    public EventStoreConnectionComponent(String id, String label, EventSource eventSource) {
        super(id, label);
        this.eventSource = eventSource;
    }

    @Override
    public Report getReport() {
        try {
            long before = System.currentTimeMillis();
            Optional<ResolvedEvent> maybeLastEvent = eventSource.readAll().readLastEvent();
            long after = System.currentTimeMillis();
            String durationText = (after - before) + "ms";

            return new Report(Status.OK, maybeLastEvent
                    .map(event -> format("last event read in %s: %s", durationText, event.locator()))
                    .orElse("empty event store read in " + durationText));
        } catch (RuntimeException e) {
            return new Report(Status.CRITICAL, e.getMessage());
        }
    }
}
