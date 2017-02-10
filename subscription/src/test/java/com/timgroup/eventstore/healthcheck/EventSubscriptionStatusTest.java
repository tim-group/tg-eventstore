package com.timgroup.eventstore.healthcheck;

import com.timgroup.clocks.testing.ManualClock;
import com.timgroup.eventsubscription.healthcheck.EventSubscriptionStatus;
import com.timgroup.eventsubscription.healthcheck.SubscriptionListenerAdapter;
import com.timgroup.tucker.info.Report;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;

import static com.timgroup.tucker.info.Health.State.healthy;
import static com.timgroup.tucker.info.Health.State.ill;
import static com.timgroup.tucker.info.Status.CRITICAL;
import static com.timgroup.tucker.info.Status.OK;
import static com.timgroup.tucker.info.Status.WARNING;
import static java.time.ZoneOffset.UTC;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class EventSubscriptionStatusTest {
    private final ManualClock clock = new ManualClock(Instant.now(), UTC);
    private EventSubscriptionStatus status;
    private SubscriptionListenerAdapter adapter;

    @Before
    public void setup() {
        status = new EventSubscriptionStatus("", clock, Duration.ofSeconds(123));
        adapter = new SubscriptionListenerAdapter(new TestPosition(0), singletonList(status));
    }

    @Test public void
    reports_ok_before_initial_replay_starts() {
        assertThat(status.get(), is(ill));
        assertThat(status.getReport(), is(new Report(OK, "Stale, catching up. No events processed yet. (Stale for PT0S)")));

    }

    @Test public void
    warns_if_initial_replay_not_started_and_threshold_passed() {
        clock.bumpSeconds(124);
        assertThat(status.getReport(), is(new Report(WARNING, "Stale, catching up. No events processed yet. (Stale for PT2M4S)")));
    }

    @Test public void
    reports_healthy_once_initial_replay_is_completed() {
        adapter.chaserReceived(new TestPosition(1));
        adapter.chaserReceived(new TestPosition(2));
        adapter.chaserReceived(new TestPosition(3));
        adapter.chaserUpToDate(new TestPosition(3));

        assertThat(status.get(), is (ill));
        assertThat(status.getReport(), is(new Report(OK, "Stale, catching up. No events processed yet. (Stale for PT0S)")));

        clock.bumpSeconds(100);
        adapter.eventProcessed(new TestPosition(1));
        adapter.eventProcessed(new TestPosition(2));
        adapter.eventProcessed(new TestPosition(3));

        assertThat(status.get(), is (healthy));
        assertThat(status.getReport(), is(new Report(OK, "Caught up at version 3. Initial replay took PT1M40S")));
    }

    @Test public void
    reports_warning_if_initial_replay_took_longer_than_maximum_configured_duration() {
        adapter.chaserReceived(new TestPosition(1));
        adapter.chaserReceived(new TestPosition(2));
        adapter.chaserReceived(new TestPosition(3));
        adapter.chaserUpToDate(new TestPosition(3));

        clock.bumpSeconds(124);
        adapter.eventProcessed(new TestPosition(1));
        adapter.eventProcessed(new TestPosition(2));
        adapter.eventProcessed(new TestPosition(3));


        assertThat(status.get(), is (healthy));
        assertThat(status.getReport(), is(new Report(WARNING, "Caught up at version 3. Initial replay took PT2M4S; this is longer than expected limit of PT2M3S.")));
    }

    @Test public void
    reports_warning_if_stale() {
        adapter.chaserUpToDate(new TestPosition(5));
        adapter.eventProcessed(new TestPosition(5));
        adapter.chaserReceived(new TestPosition(6));
        clock.bumpSeconds(7);

        assertThat(status.getReport(), is(new Report(WARNING, "Stale, catching up. Currently at version 5. (Stale for PT7S)")));
    }

    @Test public void
    reports_critical_if_stale_for_over_30s() {
        adapter.chaserUpToDate(new TestPosition(5));
        adapter.eventProcessed(new TestPosition(5));
        adapter.chaserReceived(new TestPosition(6));

        clock.bumpSeconds(31);
        assertThat(status.getReport(), is(new Report(CRITICAL, "Stale, catching up. Currently at version 5. (Stale for PT31S)")));
    }

    @Test public void
    reports_OK_if_stale_for_over_30s_during_catchup_if_under_configured_maximum_startup_duration() {
        adapter.chaserReceived(new TestPosition(1));
        adapter.eventProcessed(new TestPosition(1));

        clock.bumpSeconds(123);
        assertThat(status.getReport(), is(new Report(OK, "Stale, catching up. Currently at version 1. (Stale for PT2M3S)")));
    }

    @Test public void
    reports_ok_if_stale_for_less_than_a_second_during_catchup() {
        adapter.chaserReceived(new TestPosition(1));
        adapter.eventProcessed(new TestPosition(1));

        clock.bumpMillis(500);
        assertThat(status.getReport(), is(new Report(OK, "Stale, catching up. Currently at version 1. (Stale for PT0.5S)")));
    }

    @Test public void
    reports_warning_if_stale_for_over_configured_maximum_duration_during_initial_catchup() {
        adapter.chaserReceived(new TestPosition(1));
        adapter.eventProcessed(new TestPosition(1));

        clock.bumpSeconds(124);
        assertThat(status.getReport(), is(new Report(WARNING, "Stale, catching up. Currently at version 1. (Stale for PT2M4S)")));
    }

    @Test public void
    reports_warning_if_stale_for_more_than_25_pct_over_configured_maximum_duration_during_initial_catchup() {
        adapter.chaserReceived(new TestPosition(1));
        adapter.eventProcessed(new TestPosition(1));

        clock.bumpSeconds(800);
        assertThat(status.getReport(), is(new Report(CRITICAL, "Stale, catching up. Currently at version 1. (Stale for PT13M20S)")));
    }

    @Test public void
    reports_OK_once_caught_up_again() {
        adapter.chaserReceived(new TestPosition(1));
        adapter.chaserReceived(new TestPosition(2));
        adapter.chaserUpToDate(new TestPosition(2));
        adapter.eventProcessed(new TestPosition(1));
        adapter.eventProcessed(new TestPosition(2));

        assertThat(status.getReport().getStatus(), is(OK));
    }

    @Test public void
    reports_failure_if_subscription_terminates_due_to_event_handler_failure() {
        adapter.chaserReceived(new TestPosition(1));
        adapter.chaserUpToDate(new TestPosition(1));
        adapter.eventProcessingFailed(new TestPosition(1), new RuntimeException("Failure from handler"));

        assertThat(status.getReport().getStatus(), is(CRITICAL));
        assertThat(status.getReport().getValue().toString(), containsString("Event subscription terminated. Failed to process version 1: Failure from handler"));
        assertThat(status.getReport().getValue().toString(), containsString("EventSubscriptionStatusTest"));
    }

    @Test public void
    reports_failure_if_subscription_terminates_due_to_deserialization_failure() {
        adapter.chaserReceived(new TestPosition(1));
        adapter.chaserUpToDate(new TestPosition(1));
        adapter.eventDeserializationFailed(new TestPosition(1), new RuntimeException("Failure from deserialization"));

        assertThat(status.getReport().getStatus(), is(CRITICAL));
        assertThat(status.getReport().getValue().toString(), containsString("Event subscription terminated. Failed to process version 1: Failure from deserialization"));
        assertThat(status.getReport().getValue().toString(), containsString("EventSubscriptionStatusTest"));
    }
}
