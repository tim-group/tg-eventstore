package com.timgroup.eventsubscription.healthcheck;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

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
    private final Clock clock;
    private volatile Optional<Instant> lastPollTimestamp = Optional.empty();
    private volatile Position currentPosition = null;

    public ChaserHealth(String name, Clock clock) {
        super("event-store-chaser-" + name, "Eventstore chaser health (" + name + ")");
        this.clock = clock;
    }

    @Override
    public Report getReport() {
        if (lastPollTimestamp.isPresent()) {
            Instant timestamp = lastPollTimestamp.get();
            long seconds = Duration.between(timestamp, clock.instant()).getSeconds();
            if (seconds > 30) {
                return new Report(CRITICAL, format("potentially stale. Last up-to-date at at %s. (%s seconds ago).", timestamp, seconds));
            } else if (seconds > 5) {
                return new Report(WARNING, format("potentially stale. Last up-to-date at at %s. (%s seconds ago).", timestamp, seconds));
            } else {
                return new Report(OK, format("up-to-date at at %s. (%s seconds ago). Current version: %s", timestamp, seconds, currentPosition));
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
        lastPollTimestamp = Optional.of(clock.instant());
    }
}
