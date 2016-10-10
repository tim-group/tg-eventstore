package com.timgroup.eventsubscription.healthcheck;

import com.timgroup.eventstore.api.Position;
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
    private final Clock clock;
    private final int maxInitialReplayDuration;

    private volatile Instant startTime;
    private volatile Optional<Long> initialReplayDuration = Optional.empty();
    private volatile Optional<Position> currentPosition = Optional.empty();
    private volatile Optional<Report> terminatedReport = Optional.empty();
    private volatile Optional<Instant> staleSince = Optional.empty();

    public EventSubscriptionStatus(String name, Clock clock, int maxInitialReplayDuration) {
        super("event-subscription-status-" + name, "Event subscription status (" + name + ")");
        this.clock = clock;
        this.maxInitialReplayDuration = maxInitialReplayDuration;
        this.startTime = clock.instant();
    }

    @Override
    public Report getReport() {
        if (terminatedReport.isPresent()) {
            return terminatedReport.get();
        }

        if (staleSince.isPresent()) {
            long staleSeconds = Duration.between(staleSince.get(), clock.instant()).getSeconds();
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
            initialReplayDuration = Optional.of(Duration.between(startTime, clock.instant()).getSeconds());
        }

        staleSince = Optional.empty();

        currentPosition = Optional.of(position);
    }

    @Override
    public void staleAtVersion(Optional<Position> position) {
        if (!staleSince.isPresent()) {
            staleSince = Optional.of(clock.instant());
        }
        currentPosition = position;
    }

    @Override
    public void terminated(Position position, Exception e) {
        terminatedReport = Optional.of(new Report(CRITICAL, "Event subscription terminated. Failed to process version " + position + ": " + e.getMessage() + " at " + e.getStackTrace()[0]));
    }

    public void reset() {
        startTime = clock.instant();
        staleSince = Optional.empty();
        currentPosition = Optional.empty();
        initialReplayDuration = Optional.empty();
        terminatedReport = Optional.empty();
    }
}
