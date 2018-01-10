package com.timgroup.eventsubscription.healthcheck;

import com.timgroup.clocks.testing.ManualClock;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;

import static com.timgroup.tucker.info.Status.CRITICAL;
import static com.timgroup.tucker.info.Status.INFO;
import static com.timgroup.tucker.info.Status.OK;
import static com.timgroup.tucker.info.Status.WARNING;
import static java.time.ZoneOffset.UTC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class ChaserHealthTest {
    private final ManualClock clock = new ManualClock(Instant.now(), UTC);

    private final ChaserHealth defaultChaserHealth = new ChaserHealth("", "", clock);
    private final ChaserHealth slowChaserHealth = new ChaserHealth("", "", clock, Duration.ofSeconds(20));

    @Test public void
    reports_OK_if_chaser_polled_within_last_5s() {
        defaultChaserHealth.chaserUpToDate(new TestPosition(123));

        clock.bumpSeconds(4);

        assertThat(defaultChaserHealth.getReport().getStatus(), is(OK));
        assertThat(defaultChaserHealth.getReport().getValue().toString(), containsString("Current version: 123"));
    }

    @Test public void
    reports_OK_if_chaser_polled_within_5_times_subscriptionRunFrequency() {
        slowChaserHealth.chaserUpToDate(new TestPosition(123));

        clock.bumpSeconds(100);

        assertThat(slowChaserHealth.getReport().getStatus(), is(OK));
        assertThat(slowChaserHealth.getReport().getValue().toString(), containsString("Current version: 123"));
    }

    @Test public void
    reports_WARNING_if_chaser_did_not_poll_in_over_5s() {
        defaultChaserHealth.chaserUpToDate(new TestPosition(1));

        clock.bumpSeconds(6);
        assertThat(defaultChaserHealth.getReport().getStatus(), is(WARNING));
    }

    @Test public void
    reports_WARNING_if_chaser_did_not_poll_within_5_times_subscriptionRunFrequency() {
        slowChaserHealth.chaserUpToDate(new TestPosition(1));

        clock.bumpSeconds(101);
        assertThat(slowChaserHealth.getReport().getStatus(), is(WARNING));
    }

    @Test public void
    reports_CRITICAL_if_chaser_did_not_poll_in_over_30s() {
        defaultChaserHealth.chaserUpToDate(new TestPosition(1));

        clock.bumpSeconds(31);
        assertThat(defaultChaserHealth.getReport().getStatus(), is(CRITICAL));
    }

    @Test public void
    reports_CRITICAL_if_chaser_did_not_poll_within_10_times_subscriptionRunFrequency() {
        slowChaserHealth.chaserUpToDate(new TestPosition(1));

        clock.bumpSeconds(201);
        assertThat(slowChaserHealth.getReport().getStatus(), is(CRITICAL));
    }

    @Test public void
    does_not_report_critical_if_initial_replay_takes_longer_than_30s() {
        defaultChaserHealth.chaserReceived(new TestPosition(1));

        clock.bumpSeconds(31);
        assertThat(defaultChaserHealth.getReport().getStatus(), is(INFO));
    }

    @Test public void
    does_not_report_critical_if_initial_replay_takes_longer_than_10_times_subscriptionRunFrequency() {
        slowChaserHealth.chaserReceived(new TestPosition(1));

        clock.bumpSeconds(201);
        assertThat(slowChaserHealth.getReport().getStatus(), is(INFO));
    }

}
