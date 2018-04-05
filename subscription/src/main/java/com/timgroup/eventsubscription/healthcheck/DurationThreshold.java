package com.timgroup.eventsubscription.healthcheck;

import com.timgroup.tucker.info.Report;
import com.timgroup.tucker.info.Status;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class DurationThreshold {
    private final Duration warning;
    private final Duration critical;

    public DurationThreshold(Duration warning, Duration critical) {
        this.warning = requireNonNull(warning);
        this.critical = requireNonNull(critical);
        if (critical.compareTo(warning) < 0)
            throw new IllegalArgumentException("Critical threshold must not be less than warning threshold");
    }

    public static DurationThreshold warningThresholdWithCriticalRatio(Duration warning, double criticalRatio) {
        if (criticalRatio < 1.0) throw new IllegalArgumentException("Critical ratio should be over 1.0");
        return new DurationThreshold(warning, Duration.ofMillis(Math.round(warning.toMillis() * criticalRatio)));
    }

    public Status classify(Duration duration) {
        if (duration.compareTo(critical) > 0)
            return Status.CRITICAL;
        else if (duration.compareTo(warning) > 0)
            return Status.WARNING;
        else
            return Status.OK;
    }

    public Report classify(Duration duration, Object value) {
        return new Report(classify(duration), value);
    }

    public Duration getWarning() {
        return warning;
    }

    public Duration getCritical() {
        return critical;
    }

    public DurationThreshold withWarning(Duration threshold) {
        return new DurationThreshold(threshold, critical);
    }

    public DurationThreshold withCritical(Duration threshold) {
        return new DurationThreshold(warning, threshold);
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DurationThreshold that = (DurationThreshold) o;
        return Objects.equals(warning, that.warning) &&
                Objects.equals(critical, that.critical);
    }

    @Override
    public int hashCode() {
        return Objects.hash(warning, critical);
    }

    @Override
    public String toString() {
        return "DurationThreshold{" +
                "warning=" + warning +
                ", critical=" + critical +
                '}';
    }
}
