package com.timgroup.eventsubscription.healthcheck;

import com.timgroup.clocks.testing.ManualClock;
import com.timgroup.eventsubscription.Event;
import com.timgroup.eventsubscription.lifecycleevents.CaughtUp;
import com.timgroup.eventsubscription.lifecycleevents.InitialCatchupCompleted;
import com.timgroup.eventsubscription.lifecycleevents.SubscriptionTerminated;
import com.timgroup.structuredevents.LocalEventSink;
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class EventSubscriptionStatusTest {
    private final ManualClock clock = new ManualClock(Instant.now(), UTC);
    private EventSubscriptionStatus status;

    @Before
    public void setup() {
        status = createStatus();
        status.notifyStarted();
    }

    @Test public void
    reports_ok_before_initial_replay_starts() {
        assertThat(status.get(), is(ill));
        assertThat(status.getReport(), is(new Report(OK, "Awaiting initial catchup. No events processed yet. (Stale for PT0S)")));

    }

    @Test public void
    warns_if_initial_replay_not_started_and_threshold_passed() {
        clock.bumpSeconds(124);
        assertThat(status.getReport(), is(new Report(WARNING, "Awaiting initial catchup. No events processed yet. (Stale for PT2M4S)")));
    }

    @Test public void
    reports_healthy_once_initial_replay_is_completed() {
        assertThat(status.get(), is (ill));
        assertThat(status.getReport(), is(new Report(OK, "Awaiting initial catchup. No events processed yet. (Stale for PT0S)")));

        status.apply(new TestPosition(1), new Event() {});
        status.apply(new TestPosition(2), new Event() {});
        status.apply(new TestPosition(3), new Event() {});

        assertThat(status.get(), is (ill));
        assertThat(status.getReport(), is(new Report(OK, "Awaiting initial catchup. Currently at position 3. (Stale for PT0S)")));

        clock.bumpSeconds(100);

        status.apply(new TestPosition(3), new InitialCatchupCompleted(new TestPosition(3), clock.instant()));

        assertThat(status.get(), is (healthy));
        assertThat(status.getReport(), is(new Report(OK, "Caught up at position 3. Initial replay took PT1M40S")));
    }

    @Test public void
    reports_warning_if_initial_replay_took_longer_than_maximum_configured_duration() {
        status.apply(new TestPosition(1), new Event() {});
        status.apply(new TestPosition(2), new Event() {});
        status.apply(new TestPosition(3), new Event() {});

        clock.bumpSeconds(124);
        status.apply(new TestPosition(3), new InitialCatchupCompleted(new TestPosition(3), clock.instant()));

        assertThat(status.get(), is (healthy));
        assertThat(status.getReport(), is(new Report(WARNING, "Caught up at position 3. Initial replay took PT2M4S; this is longer than expected limit of PT2M3S.")));
    }

    @Test public void
    reports_warning_if_stale() {
        status.apply(new TestPosition(5), new InitialCatchupCompleted(new TestPosition(3), clock.instant()));
        status.apply(new TestPosition(6), new Event() {});
        clock.bumpSeconds(7);

        assertThat(status.getReport(), is(new Report(WARNING, "Stale, catching up. Currently at position 6. (Stale for PT7S)")));
    }

    @Test public void
    reports_critical_if_stale_for_over_30s() {
        status.apply(new TestPosition(5), new InitialCatchupCompleted(new TestPosition(5), clock.instant()));
        status.apply(new TestPosition(6), new Event() {});

        clock.bumpSeconds(31);
        assertThat(status.getReport(), is(new Report(CRITICAL, "Stale, catching up. Currently at position 6. (Stale for PT31S)")));
    }

    @Test public void
    reports_OK_if_stale_for_over_30s_during_catchup_if_under_configured_maximum_startup_duration() {
        status.apply(new TestPosition(1), new Event() {});

        clock.bumpSeconds(123);
        assertThat(status.getReport(), is(new Report(OK, "Awaiting initial catchup. Currently at position 1. (Stale for PT2M3S)")));
    }

    @Test public void
    reports_ok_if_stale_for_less_than_a_second_during_catchup() {
        status.apply(new TestPosition(1), new Event() {});

        clock.bumpMillis(500);
        assertThat(status.getReport(), is(new Report(OK, "Awaiting initial catchup. Currently at position 1. (Stale for PT0.5S)")));
    }

    @Test public void
    reports_warning_if_stale_for_over_configured_maximum_duration_during_initial_catchup() {
        status.apply(new TestPosition(1), new Event() {});

        clock.bumpSeconds(124);
        assertThat(status.getReport(), is(new Report(WARNING, "Awaiting initial catchup. Currently at position 1. (Stale for PT2M4S)")));
    }

    @Test public void
    reports_warning_if_stale_for_more_than_25_pct_over_configured_maximum_duration_during_initial_catchup() {
        status.apply(new TestPosition(1), new Event() {});

        clock.bumpSeconds(800);
        assertThat(status.getReport(), is(new Report(CRITICAL, "Awaiting initial catchup. Currently at position 1. (Stale for PT13M20S)")));
    }

    @Test public void
    reports_OK_once_caught_up_again() {
        status.apply(new TestPosition(1), new InitialCatchupCompleted(new TestPosition(1), clock.instant()));

        clock.bumpSeconds(2);
        assertThat(status.getReport().getStatus(), is(WARNING));

        status.apply(new TestPosition(1), new CaughtUp(new TestPosition(1), clock.instant()));

        assertThat(status.getReport().getStatus(), is(OK));
    }

    @Test public void
    reports_failure_if_subscription_terminates_due_to_event_handler_failure() {
        status.apply(new TestPosition(1), new InitialCatchupCompleted(new TestPosition(1), clock.instant()));

        status.apply(new TestPosition(1), new SubscriptionTerminated(new TestPosition(1), new RuntimeException("Failure from handler")));

        assertThat(status.getReport().getStatus(), is(CRITICAL));
        assertThat(status.getReport().getValue().toString(), containsString("Event subscription terminated. Failed to process position 1: Failure from handler"));
        assertThat(status.getReport().getValue().toString(), containsString("EventSubscriptionStatusTest"));
    }

    @Test public void
    warns_until_subscription_is_started() {
        EventSubscriptionStatus manualStatus = createStatus();

        assertThat(manualStatus.getReport().getStatus(), is(WARNING));
        assertThat(manualStatus.getReport().getValue().toString(), is("Subscription not yet started"));

        manualStatus.notifyStarted();

        assertThat(manualStatus.getReport().getStatus(), is(OK));
    }

    private EventSubscriptionStatus createStatus() {
        return new EventSubscriptionStatus("", clock, DurationThreshold.warningThresholdWithCriticalRatio(Duration.ofSeconds(123), 1.25), new DurationThreshold(Duration.ofSeconds(1), Duration.ofSeconds(30)), new LocalEventSink());
    }
}
