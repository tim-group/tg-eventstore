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
    private static final Duration STALENESS_WARNING_THRESHOLD = Duration.ofSeconds(1);
    private static final Duration STALENESS_CRITICAL_THRESHOLD = Duration.ofSeconds(30);
    private final String name;
    private final Clock clock;
    private final Duration maxInitialReplayDuration;
    private final Duration criticalInitialReplayDuration;
    private final EventSink eventSink;

    private volatile Instant startTime;
    private volatile Duration initialReplayDuration;
    private volatile Position currentPosition;
    private volatile Report terminatedReport;
    private volatile Instant staleSince;

    public EventSubscriptionStatus(String name, String description, Clock clock, Duration maxInitialReplayDuration, EventSink eventSink) {
        super("event-subscription-status-" + name, "Event subscription status (" + name + ": " + description + ")");
        this.name = name;
        this.clock = clock;
        this.maxInitialReplayDuration = maxInitialReplayDuration;
        this.startTime = Instant.now(clock);
        this.staleSince = Instant.now(clock);
        this.criticalInitialReplayDuration = maxInitialReplayDuration.plus(maxInitialReplayDuration.dividedBy(4)); // 25% over max = critical
        this.eventSink = eventSink;
    }

    @Override
    public Report getReport() {
        if (terminatedReport != null) {
            return terminatedReport;
        }

        Instant staleSinceSnap = this.staleSince;
        if (staleSinceSnap != null) {
            Duration staleFor = Duration.between(staleSinceSnap, Instant.now(clock));
            Status status = initialReplayDuration != null ? classifyStaleness(staleFor) : classifyInitialStaleness(staleFor);

            String currentVersionText = currentPosition != null ?  "Currently at version " + currentPosition + "." : "No events processed yet.";
            return new Report(status, "Stale, catching up. " + currentVersionText + " (Stale for " + staleFor + ")");
        } else if (initialReplayDuration != null) {
            if (initialReplayDuration.compareTo(maxInitialReplayDuration) < 0) {
                return new Report(OK, "Caught up at version " + currentPosition + ". Initial replay took " + initialReplayDuration);
            } else {
                return new Report(WARNING, "Caught up at version " + currentPosition + ". Initial replay took " + initialReplayDuration + "; " +
                        "this is longer than expected limit of " + maxInitialReplayDuration + ".");
            }
        } else {
            throw new RuntimeException("Not stale and no replay completed");
        }
    }

    private Status classifyInitialStaleness(Duration staleFor) {
        return staleFor.compareTo(criticalInitialReplayDuration) > 0 ? CRITICAL
                : staleFor.compareTo(maxInitialReplayDuration) > 0 ? WARNING
                : OK;
    }

    private Status classifyStaleness(Duration staleFor) {
        return staleFor.compareTo(STALENESS_CRITICAL_THRESHOLD) > 0 ? CRITICAL
                : staleFor.compareTo(STALENESS_WARNING_THRESHOLD) > 0 ? WARNING
                : OK;
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
