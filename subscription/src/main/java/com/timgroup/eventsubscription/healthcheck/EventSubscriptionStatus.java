package com.timgroup.eventsubscription.healthcheck;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import com.timgroup.eventstore.api.Position;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.Health;
import com.timgroup.tucker.info.Report;
import com.timgroup.tucker.info.Status;

import static com.timgroup.tucker.info.Status.CRITICAL;
import static com.timgroup.tucker.info.Status.OK;
import static com.timgroup.tucker.info.Status.WARNING;

public class EventSubscriptionStatus extends Component implements Health, SubscriptionListener {
    private final Clock clock;
    private final Duration maxInitialReplayDuration;

    private volatile Instant startTime;
    private volatile Optional<Duration> initialReplayDuration = Optional.empty();
    private volatile Optional<Position> currentPosition = Optional.empty();
    private volatile Optional<Report> terminatedReport = Optional.empty();
    private volatile Optional<Instant> staleSince = Optional.empty();

    public EventSubscriptionStatus(String name, Clock clock, Duration maxInitialReplayDuration) {
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
            Duration staleFor = Duration.between(staleSince.get(), clock.instant());
            Status status = initialReplayDuration.map(s -> {
                if (staleFor.getSeconds() > 30) { return CRITICAL; } else { return WARNING; }
            }).orElse(staleFor.compareTo(maxInitialReplayDuration) > 0 ? CRITICAL : OK);

            String currentVersionText = currentPosition.map(v -> "Currently at version " + v + ".").orElse("No events processed yet.");
            return new Report(status, "Stale, catching up. " + currentVersionText + " (Stale for " + staleFor + ")");
        } else if (initialReplayDuration.isPresent()) {
            if (initialReplayDuration.get().compareTo(maxInitialReplayDuration) < 0) {
                return new Report(OK, "Caught up at version " + currentPosition.map(Object::toString).orElse("") + ". Initial replay took " + initialReplayDuration.get());
            } else {
                return new Report(WARNING, "Caught up at version " + currentPosition.map(Object::toString).orElse("") + ". Initial replay took " + initialReplayDuration.get() + "; " +
                        "this is longer than expected limit of " + maxInitialReplayDuration + ".");
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
            initialReplayDuration = Optional.of(Duration.between(startTime, clock.instant()));
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
