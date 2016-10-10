package com.timgroup.eventsubscription.healthcheck;

import com.timgroup.eventstore.api.Clock;
import com.timgroup.eventstore.api.Position;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.Health;
import com.timgroup.tucker.info.Report;
import com.timgroup.tucker.info.Status;
import org.joda.time.DateTime;

import java.util.Optional;

import static com.timgroup.tucker.info.Status.CRITICAL;
import static com.timgroup.tucker.info.Status.OK;
import static com.timgroup.tucker.info.Status.WARNING;
import static org.joda.time.Seconds.secondsBetween;

public class EventSubscriptionStatus extends Component implements Health, SubscriptionListener {
    private final Clock clock;
    private final int maxInitialReplayDuration;

    private volatile DateTime startTime;
    private volatile Optional<Integer> initialReplayDuration = Optional.empty();
    private volatile Optional<Position> currentPosition = Optional.empty();
    private volatile Optional<Report> terminatedReport = Optional.empty();
    private volatile Optional<DateTime> staleSince = Optional.empty();

    public EventSubscriptionStatus(String name, Clock clock, int maxInitialReplayDuration) {
        super("event-subscription-status-" + name, "Event subscription status (" + name + ")");
        this.clock = clock;
        this.maxInitialReplayDuration = maxInitialReplayDuration;
        this.startTime = clock.now();
    }

    @Override
    public Report getReport() {
        if (terminatedReport.isPresent()) {
            return terminatedReport.get();
        }

        if (staleSince.isPresent()) {
            long staleSeconds = secondsBetween(staleSince.get(), clock.now()).getSeconds();
            Status status = initialReplayDuration.map(s -> {
                if (staleSeconds > 30) { return CRITICAL; } else { return WARNING; }
            }).orElse(staleSeconds > maxInitialReplayDuration ? CRITICAL : OK);

            String currentVersionText = currentPosition.map(v -> "Currently at version " + v + ".").orElse("No events processed yet.");
            return new Report(status, "Stale, catching up. " + currentVersionText + " (Stale for " + staleSeconds + "s)");
        } else if (initialReplayDuration.isPresent()) {
            if (initialReplayDuration.get() < maxInitialReplayDuration) {
                return new Report(OK, "Caught up at version " + currentPosition.map(Object::toString).orElse("") + ". Initial replay took " + initialReplayDuration.get() + "s.");
            } else {
                return new Report(WARNING, "Caught up at version " + currentPosition.map(Object::toString).orElse("") + ". Initial replay took " + initialReplayDuration.get() + "s. " +
                        "This is longer than expected limit of " + maxInitialReplayDuration + "s.");
            }
        } else {
            return new Report(WARNING, "Awaiting events.");
        }
    }

    @Override
    public Health.State get() {
        return initialReplayDuration.isPresent() ? State.healthy : State.ill;
    }

    @Override
    public void caughtUpAt(Position position) {
        if (!initialReplayDuration.isPresent()) {
            initialReplayDuration = Optional.of(secondsBetween(startTime, clock.now()).getSeconds());
        }

        staleSince = Optional.empty();

        currentPosition = Optional.of(position);
    }

    @Override
    public void staleAtVersion(Optional<Position> position) {
        if (!staleSince.isPresent()) {
            staleSince = Optional.of(clock.now());
        }
        currentPosition = position;
    }

    @Override
    public void terminated(Position position, Exception e) {
        terminatedReport = Optional.of(new Report(CRITICAL, "Event subscription terminated. Failed to process version " + position + ": " + e.getMessage() + " at " + e.getStackTrace()[0]));
    }

    public void reset() {
        startTime = clock.now();
        staleSince = Optional.empty();
        currentPosition = Optional.empty();
        initialReplayDuration = Optional.empty();
        terminatedReport = Optional.empty();
    }
}
