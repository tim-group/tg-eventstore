package com.timgroup.eventstore.healthcheck;

import com.timgroup.clocks.testing.ManualClock;
import com.timgroup.eventsubscription.healthcheck.ChaserHealth;
import org.junit.Test;

import java.time.Instant;

import static com.timgroup.tucker.info.Status.CRITICAL;
import static com.timgroup.tucker.info.Status.OK;
import static com.timgroup.tucker.info.Status.WARNING;
import static java.time.ZoneOffset.UTC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class ChaserHealthTest {
    private final ManualClock clock = new ManualClock(Instant.now(), UTC);

    private final ChaserHealth chaserHealth = new ChaserHealth("", clock);

    @Test public void
    reports_OK_if_chaser_polled_within_last_5s() {
        chaserHealth.chaserUpToDate(new TestPosition(123));

        clock.bumpSeconds(4);

        assertThat(chaserHealth.getReport().getStatus(), is(OK));
        assertThat(chaserHealth.getReport().getValue().toString(), containsString("Current version: 123"));
    }

    @Test public void
    reports_WARNING_if_chaser_did_not_poll_in_over_5s() {
        chaserHealth.chaserUpToDate(new TestPosition(1));

        clock.bumpSeconds(6);
        assertThat(chaserHealth.getReport().getStatus(), is(WARNING));
    }

    @Test public void
    reports_CRITICAL_if_chaser_did_not_poll_in_over_30s() {
        chaserHealth.chaserUpToDate(new TestPosition(1));

        clock.bumpSeconds(31);
        assertThat(chaserHealth.getReport().getStatus(), is(CRITICAL));
    }

    @Test public void
    does_not_report_critical_if_initial_replay_takes_longer_than_30s() {
        chaserHealth.chaserReceived(new TestPosition(1));

        clock.bumpSeconds(31);
        assertThat(chaserHealth.getReport().getStatus(), is(WARNING));
    }

}
