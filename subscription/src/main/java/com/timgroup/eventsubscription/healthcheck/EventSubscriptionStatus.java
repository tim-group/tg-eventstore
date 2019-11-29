package com.timgroup.eventsubscription.healthcheck;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventsubscription.Event;
import com.timgroup.eventsubscription.EventHandler;
import com.timgroup.eventsubscription.lifecycleevents.CaughtUp;
import com.timgroup.eventsubscription.lifecycleevents.InitialCatchupCompleted;
import com.timgroup.eventsubscription.lifecycleevents.SubscriptionTerminated;
import com.timgroup.structuredevents.EventSink;
import com.timgroup.structuredevents.RetentionPeriod;
import com.timgroup.structuredevents.SimpleEvent;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.Health;
import com.timgroup.tucker.info.Report;
import com.timgroup.tucker.info.Status;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import static com.timgroup.tucker.info.Status.CRITICAL;
import static com.timgroup.tucker.info.Status.OK;
import static com.timgroup.tucker.info.Status.WARNING;

public final class EventSubscriptionStatus extends Component implements Health, EventHandler {
    private final String name;
    private final Clock clock;
    private final EventSink eventSink;
    private final DurationThreshold initialReplay;
    private final DurationThreshold staleness;

    private volatile Instant startTime;
    private volatile Duration initialReplayDuration;
    private volatile Position currentPosition;
    private volatile Report terminatedReport;
    private volatile Instant lastCaughtUpAt;


    public EventSubscriptionStatus(String name, Clock clock, DurationThreshold initialReplay, DurationThreshold staleness, EventSink eventSink) {
        super("event-subscription-status-" + name, "Event subscription status (" + name + ")");
        this.name = name;
        this.clock = clock;
        this.staleness = staleness;
        this.initialReplay = initialReplay;
        this.lastCaughtUpAt = Instant.now(clock);
        this.eventSink = eventSink;
    }

    public void notifyStarted() {
        startTime = Instant.now(clock);
    }

    @Override
    public Report getReport() {
        if (terminatedReport != null) {
            return terminatedReport;
        }

        if (startTime == null) {
            return new Report(WARNING, "Subscription not yet started");
        }

        Duration staleFor = Duration.between(lastCaughtUpAt, Instant.now(clock));
        if (initialReplayDuration == null) {
            Status status = initialReplay.classify(staleFor);
            String currentVersionText = currentPosition != null ? "Currently at position " + currentPosition + "." : "No events processed yet.";

            return new Report(status, "Awaiting initial catchup. " + currentVersionText + " (Stale for " + staleFor + ")");
        } else {
            Status status = staleness.classify(staleFor);
            if (status == Status.OK) {
                if (initialReplayDuration.compareTo(initialReplay.getWarning()) < 0) {
                    return new Report(OK, "Caught up at position " + currentPosition + ". Initial replay took " + initialReplayDuration);
                } else {
                    return new Report(WARNING, "Caught up at position " + currentPosition + ". Initial replay took " + initialReplayDuration + "; " +
                            "this is longer than expected limit of " + initialReplay.getWarning() + ".");
                }
            } else {
                String currentVersionText = currentPosition != null ? "Currently at position " + currentPosition + "." : "No events processed yet.";
                return new Report(status, "Stale, catching up. " + currentVersionText + " (Stale for " + staleFor + ")");
            }
        }
    }

    @Override
    public Health.State get() {
        return initialReplayDuration != null ? State.healthy : State.ill;
    }


    @Override
    public void apply(Position position, Event event) {
        currentPosition = position;
        if (event instanceof InitialCatchupCompleted) {
            initialReplayDuration = Duration.between(startTime, Instant.now(clock));
            eventSink.sendEvent(SimpleEvent.ofType("InitialReplayCompleted").withRetention(RetentionPeriod.LONG).withField("name", name).withField("time_ms", initialReplayDuration.toMillis()));
            lastCaughtUpAt = ((InitialCatchupCompleted) event).timestamp();
        } else if (event instanceof CaughtUp) {
            lastCaughtUpAt = ((CaughtUp) event).timestamp();
        } else if (event instanceof SubscriptionTerminated) {
            Exception e = ((SubscriptionTerminated) event).exception;
            terminatedReport = new Report(CRITICAL, "Event subscription terminated. Failed to process position " + position + ": " + e.getMessage() + " at " + e.getStackTrace()[0]);
        }
    }
}
