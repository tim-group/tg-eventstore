package com.timgroup.eventstore.healthcheck;

import com.timgroup.eventsubscription.healthcheck.ChaserHealth;
import org.junit.Test;

import java.time.Clock;
import java.time.Instant;

import static com.timgroup.tucker.info.Status.CRITICAL;
import static com.timgroup.tucker.info.Status.OK;
import static com.timgroup.tucker.info.Status.WARNING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ChaserHealthTest {
    private final Clock clock = mock(Clock.class);
    private final Instant now = Instant.now();

    private final ChaserHealth chaserHealth = new ChaserHealth("", clock);

    @Test public void
    reports_OK_if_chaser_polled_within_last_5s() {
        when(clock.instant()).thenReturn(now);

        chaserHealth.chaserUpToDate(new TestPosition(123));

        when(clock.instant()).thenReturn(now.plusSeconds(4));

        assertThat(chaserHealth.getReport().getStatus(), is(OK));
        assertThat(chaserHealth.getReport().getValue().toString(), containsString("Current version: 123"));
    }

    @Test public void
    reports_WARNING_if_chaser_did_not_poll_in_over_5s() {
        when(clock.instant()).thenReturn(now);
        chaserHealth.chaserUpToDate(new TestPosition(1));

        when(clock.instant()).thenReturn(now.plusSeconds(6));
        assertThat(chaserHealth.getReport().getStatus(), is(WARNING));
    }

    @Test public void
    reports_CRITICAL_if_chaser_did_not_poll_in_over_30s() {
        when(clock.instant()).thenReturn(now);
        chaserHealth.chaserUpToDate(new TestPosition(1));

        when(clock.instant()).thenReturn(now.plusSeconds(31));
        assertThat(chaserHealth.getReport().getStatus(), is(CRITICAL));
    }

    @Test public void
    does_not_report_critical_if_initial_replay_takes_longer_than_30s() {
        when(clock.instant()).thenReturn(now);
        chaserHealth.chaserReceived(new TestPosition(1));

        when(clock.instant()).thenReturn(now.plusSeconds(31));
        assertThat(chaserHealth.getReport().getStatus(), is(WARNING));
    }

}
