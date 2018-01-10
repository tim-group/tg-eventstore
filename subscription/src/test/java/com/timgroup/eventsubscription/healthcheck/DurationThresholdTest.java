package com.timgroup.eventsubscription.healthcheck;

import com.timgroup.tucker.info.Status;
import org.junit.Test;

import java.time.Duration;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class DurationThresholdTest {
    @Test
    public void classifies_durations() {
        DurationThreshold threshold = new DurationThreshold(Duration.ofSeconds(1), Duration.ofSeconds(5));
        assertThat(threshold.classify(Duration.ofMillis(500L)), equalTo(Status.OK));
        assertThat(threshold.classify(Duration.ofMillis(1000L)), equalTo(Status.OK)); // exclusive boundary
        assertThat(threshold.classify(Duration.ofMillis(1500L)), equalTo(Status.WARNING));
        assertThat(threshold.classify(Duration.ofMillis(5000L)), equalTo(Status.WARNING)); // exclusive boundary
        assertThat(threshold.classify(Duration.ofMillis(5500L)), equalTo(Status.CRITICAL));
    }

    @Test
    public void creates_threshold_using_ratio() {
        assertThat(DurationThreshold.warningThresholdWithCriticalRatio(Duration.ofSeconds(4), 1.25),
                equalTo(new DurationThreshold(Duration.ofSeconds(4), Duration.ofSeconds(5))));
    }

    @Test
    public void critical_threshold_can_be_equal_to_warning() {
        assertThat(new DurationThreshold(Duration.ofSeconds(1), Duration.ofSeconds(1)).classify(Duration.ofSeconds(1)), equalTo(Status.OK));
        assertThat(new DurationThreshold(Duration.ofSeconds(1), Duration.ofSeconds(1)).classify(Duration.ofSeconds(2)), equalTo(Status.CRITICAL));
        assertThat(DurationThreshold.warningThresholdWithCriticalRatio(Duration.ofSeconds(1), 1.0).classify(Duration.ofSeconds(2)), equalTo(Status.CRITICAL));
    }

    @Test(expected = IllegalArgumentException.class)
    public void critical_threshold_may_not_be_less_than_warning() {
        new DurationThreshold(Duration.ofSeconds(2), Duration.ofSeconds(1));
    }
}