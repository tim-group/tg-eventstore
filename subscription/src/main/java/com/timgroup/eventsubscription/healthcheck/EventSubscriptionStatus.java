package com.timgroup.eventsubscription.healthcheck;

import com.timgroup.eventstore.api.Position;
import com.timgroup.structuredevents.EventSink;
import com.timgroup.structuredevents.SimpleEvent;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.Health;
import com.timgroup.tucker.info.Report;
import com.timgroup.tucker.info.Status;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static com.timgroup.tucker.info.Status.CRITICAL;
import static com.timgroup.tucker.info.Status.OK;
import static com.timgroup.tucker.info.Status.WARNING;

public class EventSubscriptionStatus extends Component implements Health, SubscriptionListener {
    private final String name;
    private final Clock clock;
    private final EventSink eventSink;
    private final DurationThreshold initialReplay;
    private final DurationThreshold staleness;

    private volatile Instant startTime;
    private volatile Duration initialReplayDuration;
    private volatile Position currentPosition;
    private volatile Report terminatedReport;
    private volatile Instant staleSince;

    public EventSubscriptionStatus(String name, Clock clock, DurationThreshold initialReplay, DurationThreshold staleness, EventSink eventSink) {
        super("event-subscription-status-" + name, "Event subscription status (" + name + ")");
        this.name = name;
        this.clock = clock;
        this.staleness = staleness;
        this.initialReplay = initialReplay;
        this.startTime = Instant.now(clock);
        this.staleSince = Instant.now(clock);
        this.eventSink = eventSink;
    }

    /**
     * Use constructor with description instead
     */
    @Deprecated
    public EventSubscriptionStatus(String name, Clock clock, Duration maxInitialReplayDuration, EventSink eventSink) {
        this(name, clock, DurationThreshold.warningThresholdWithCriticalRatio(maxInitialReplayDuration, 1.25), new DurationThreshold(Duration.ofSeconds(1), Duration.ofSeconds(30)), eventSink);
    }

    @Override
    public Report getReport() {
        if (terminatedReport != null) {
            return terminatedReport;
        }

        Instant staleSinceSnap = this.staleSince;
        if (staleSinceSnap != null) {
            Duration staleFor = Duration.between(staleSinceSnap, Instant.now(clock));
            Status status = initialReplayDuration != null ? staleness.classify(staleFor) : initialReplay.classify(staleFor);

            String currentVersionText = currentPosition != null ?  "Currently at version " + currentPosition + "." : "No events processed yet.";
            return new Report(status, "Stale, catching up. " + currentVersionText + " (Stale for " + staleFor + ")");
        } else if (initialReplayDuration != null) {
            if (initialReplayDuration.compareTo(initialReplay.getWarning()) < 0) {
                return new Report(OK, "Caught up at version " + currentPosition + ". Initial replay took " + initialReplayDuration);
            } else {
                return new Report(WARNING, "Caught up at version " + currentPosition + ". Initial replay took " + initialReplayDuration + "; " +
                        "this is longer than expected limit of " + initialReplay.getWarning() + ".");
            }
        } else {
            throw new RuntimeException("Not stale and no replay completed");
        }
    }

    @Override
    public Health.State get() {
        return initialReplayDuration != null ? State.healthy : State.ill;
    }

    @Override
    public void caughtUpAt(Position position) {
        if (initialReplayDuration == null) {
            initialReplayDuration = Duration.between(startTime, Instant.now(clock));
            eventSink.sendEvent(SimpleEvent.ofType("InitialReplayCompleted").withField("name", name).withField("time_ms", initialReplayDuration.toMillis()));
        }

        staleSince = null;

        currentPosition = position;
    }

    @Override
    public void staleAtVersion(Optional<Position> position) {
        if (staleSince == null) {
            staleSince = Instant.now(clock);
        }
        currentPosition = position.orElse(null);
    }

    @Override
    public void terminated(Position position, Exception e) {
        terminatedReport = new Report(CRITICAL, "Event subscription terminated. Failed to process version " + position + ": " + e.getMessage() + " at " + e.getStackTrace()[0]);
    }

    public void reset() {
        startTime = Instant.now(clock);
        staleSince = Instant.now(clock);
        currentPosition = null;
        initialReplayDuration = null;
        terminatedReport = null;
    }
}
