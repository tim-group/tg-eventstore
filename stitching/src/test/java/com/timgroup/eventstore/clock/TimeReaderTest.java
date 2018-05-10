package com.timgroup.eventstore.clock;

import com.timgroup.clocks.testing.ManualClock;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class TimeReaderTest {
    private final ManualClock clock = ManualClock.initiallyAt(Clock.tick(Clock.systemUTC(), Duration.ofMillis(1)));
    private final Instant start = clock.instant();
    private final TimeReader reader = new TimeReader(clock.instant(), Duration.ofSeconds(10), clock);

    @Test
    public void
    when_less_time_than_accuracy_has_passed_no_events() {
        assertThat(reader.readAllForwards().count(), is(0L));
        clock.bump(Duration.ofMillis(9999));
        assertThat(reader.readAllForwards().count(), is(0L));
    }

    @Test
    public void
    when_enough_time_has_passed_events() {
        clock.bump(Duration.ofMillis(39999));
        assertThat(reader.readAllForwards().map(r -> r.eventRecord().timestamp()).collect(toList()), Matchers.contains(
                start.plusMillis(10000),
                start.plusMillis(20000),
                start.plusMillis(30000)
        ));
    }
}