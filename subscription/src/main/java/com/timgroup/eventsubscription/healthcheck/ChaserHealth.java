package com.timgroup.eventsubscription.healthcheck;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventsubscription.ChaserListener;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.Report;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.timgroup.tucker.info.Status.CRITICAL;
import static com.timgroup.tucker.info.Status.INFO;
import static com.timgroup.tucker.info.Status.OK;
import static com.timgroup.tucker.info.Status.WARNING;
import static java.lang.String.format;

public class ChaserHealth extends Component implements ChaserListener {
    private static final Logger LOG = LoggerFactory.getLogger(ChaserHealth.class);
    private static final Duration WARNING_THRESHOLD = Duration.ofSeconds(5);
    private static final Duration CRITICAL_THRESHOLD = Duration.ofSeconds(30);
    private final Clock clock;
    private volatile Instant lastPollTimestamp;
    private volatile Position currentPosition;

    public ChaserHealth(String name, Clock clock) {
        super("event-store-chaser-" + name, "Eventstore chaser health (" + name + ")");
        this.clock = clock;
    }

    @Override
    public Report getReport() {
        Instant timestamp = lastPollTimestamp;
        if (timestamp != null) {
            Duration duration = Duration.between(timestamp, Instant.now(clock));
            if (duration.compareTo(CRITICAL_THRESHOLD) > 0) {
                return new Report(CRITICAL, format("potentially stale. Last up-to-date at at %s. (%s ago).", timestamp, duration));
            } else if (duration.compareTo(WARNING_THRESHOLD) > 0) {
                return new Report(WARNING, format("potentially stale. Last up-to-date at at %s. (%s ago).", timestamp, duration));
            } else {
                return new Report(OK, format("up-to-date at at %s. (%s ago). Current version: %s", timestamp, duration, currentPosition));
            }
        }

        return new Report(INFO, "Awaiting initial catchup. Current version: " + currentPosition);
    }

    @Override
    public void transientFailure(Exception e) {
        LOG.warn("Failure chasing eventstream.", e);
    }

    @Override
    public void chaserReceived(Position position) {
        currentPosition = position;
    }

    @Override
    public void chaserUpToDate(Position position) {
        currentPosition = position;
        lastPollTimestamp = Instant.now(clock);
    }
}
