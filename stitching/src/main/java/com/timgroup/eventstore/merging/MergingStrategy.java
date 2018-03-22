package com.timgroup.eventstore.merging;

import com.timgroup.eventstore.api.ResolvedEvent;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public interface MergingStrategy<T extends Comparable<T>> {

    @Nonnull T toComparable(@Nonnull ResolvedEvent event);

    @Nonnull default Duration delay() { return Duration.ZERO; }

    @Nonnull default MergingStrategy<T> withDelay(@Nonnull Duration delay) {
        return new DelayedMergingStrategy<>(delay, this);
    }

    final class StreamIndexMergingStrategy implements MergingStrategy<Integer> {
        @Nonnull
        @Override
        public Integer toComparable(@Nonnull ResolvedEvent event) {
            return 0;
        }
    }

    final class EffectiveTimestampMergingStrategy implements MergingStrategy<Instant> {
        private static final Pattern EFFECTIVE_TIMESTAMP_PATTERN = Pattern.compile("\"effective_timestamp\"\\s*:\\s*\"([^\"]+)\"");

        @Nonnull
        @Override
        public Instant toComparable(@Nonnull ResolvedEvent event) {
            return effectiveTimestampFrom(event);
        }

        private static Instant effectiveTimestampFrom(ResolvedEvent event) {
            String metadata = new String(event.eventRecord().metadata(), UTF_8);
            Matcher matcher = EFFECTIVE_TIMESTAMP_PATTERN.matcher(metadata);
            if (matcher.find()) {
                return Instant.parse(matcher.group(1));
            }
            throw new IllegalStateException("no timestamp in metadata of " + event);
        }
    }

    final class DelayedMergingStrategy<T extends Comparable<T>> implements MergingStrategy<T> {
        private final Duration delay;
        private final MergingStrategy<T> delegate;

        private DelayedMergingStrategy(Duration delay, MergingStrategy<T> delegate) {
            this.delay = requireNonNull(delay);
            this.delegate = requireNonNull(delegate);
        }

        @Nonnull
        @Override
        public T toComparable(@Nonnull ResolvedEvent event) {
            return delegate.toComparable(event);
        }

        @Nonnull
        @Override
        public Duration delay() {
            return this.delay;
        }
    }
}
