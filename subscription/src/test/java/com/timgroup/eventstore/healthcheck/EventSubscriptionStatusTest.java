package com.timgroup.eventstore.healthcheck;

import com.timgroup.eventsubscription.healthcheck.EventSubscriptionStatus;
import com.timgroup.eventsubscription.healthcheck.SubscriptionListenerAdapter;
import com.timgroup.tucker.info.Report;
import org.junit.Before;
import org.junit.Test;

import java.time.Clock;
import java.time.Instant;

import static com.timgroup.tucker.info.Health.State.healthy;
import static com.timgroup.tucker.info.Health.State.ill;
import static com.timgroup.tucker.info.Status.CRITICAL;
import static com.timgroup.tucker.info.Status.OK;
import static com.timgroup.tucker.info.Status.WARNING;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EventSubscriptionStatusTest {
    private final Instant now = Instant.now();
    private final Clock clock = mock(Clock.class);
    private EventSubscriptionStatus status;
    private SubscriptionListenerAdapter adapter;

    @Before
    public void setup() {
        when(clock.instant()).thenReturn(now);
        status = new EventSubscriptionStatus("", clock, 123);
        adapter = new SubscriptionListenerAdapter(new TestPosition(0), singletonList(status));
    }

    @Test public void
    reports_ill_whilst_initial_replay_is_in_progress() {
        assertThat(status.get(), is(ill));
        assertThat(status.getReport(), is(new Report(WARNING, "Awaiting events.")));
    }

    @Test public void
    reports_healthy_once_initial_replay_is_completed() {
        adapter.chaserReceived(new TestPosition(1));
        adapter.chaserReceived(new TestPosition(2));
        adapter.chaserReceived(new TestPosition(3));
        adapter.chaserUpToDate(new TestPosition(3));

        assertThat(status.get(), is (ill));
        assertThat(status.getReport(), is(new Report(OK, "Stale, catching up. No events processed yet. (Stale for 0s)")));

        when(clock.instant()).thenReturn(now.plusSeconds(100));
        adapter.eventProcessed(new TestPosition(1));
        adapter.eventProcessed(new TestPosition(2));
        adapter.eventProcessed(new TestPosition(3));

        assertThat(status.get(), is (healthy));
        assertThat(status.getReport(), is(new Report(OK, "Caught up at version 3. Initial replay took 100s.")));
    }

    @Test public void
    reports_warning_if_initial_replay_took_longer_than_maximum_configured_duration() {
        adapter.chaserReceived(new TestPosition(1));
        adapter.chaserReceived(new TestPosition(2));
        adapter.chaserReceived(new TestPosition(3));
        adapter.chaserUpToDate(new TestPosition(3));

        when(clock.instant()).thenReturn(now.plusSeconds(124));
        adapter.eventProcessed(new TestPosition(1));
        adapter.eventProcessed(new TestPosition(2));
        adapter.eventProcessed(new TestPosition(3));


        assertThat(status.get(), is (healthy));
        assertThat(status.getReport(), is(new Report(WARNING, "Caught up at version 3. Initial replay took 124s. This is longer than expected limit of 123s.")));
    }

    @Test public void
    reports_warning_if_stale() {
        adapter.chaserUpToDate(new TestPosition(5));
        adapter.eventProcessed(new TestPosition(5));
        adapter.chaserReceived(new TestPosition(6));
        when(clock.instant()).thenReturn(now.plusSeconds(7));

        assertThat(status.getReport(), is(new Report(WARNING, "Stale, catching up. Currently at version 5. (Stale for 7s)")));
    }

    @Test public void
    reports_critical_if_stale_for_over_30s() {
        adapter.chaserUpToDate(new TestPosition(5));
        adapter.eventProcessed(new TestPosition(5));
        adapter.chaserReceived(new TestPosition(6));

        when(clock.instant()).thenReturn(now.plusSeconds(31));
        assertThat(status.getReport(), is(new Report(CRITICAL, "Stale, catching up. Currently at version 5. (Stale for 31s)")));
    }

    @Test public void
    reports_OK_if_stale_for_over_30s_during_catchup_if_under_configured_maximum_startup_duration() {
        adapter.chaserReceived(new TestPosition(1));
        adapter.eventProcessed(new TestPosition(1));

        when(clock.instant()).thenReturn(now.plusSeconds(123));
        assertThat(status.getReport(), is(new Report(OK, "Stale, catching up. Currently at version 1. (Stale for 123s)")));
    }

    @Test public void
    reports_critical_if_stale_for_over_configured_maximum_duration_during_catchup() {
        adapter.chaserReceived(new TestPosition(1));
        adapter.eventProcessed(new TestPosition(1));

        when(clock.instant()).thenReturn(now.plusSeconds(124));
        assertThat(status.getReport(), is(new Report(CRITICAL, "Stale, catching up. Currently at version 1. (Stale for 124s)")));
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
