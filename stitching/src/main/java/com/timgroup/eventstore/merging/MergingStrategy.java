package com.timgroup.eventstore.merging;

import com.timgroup.eventstore.api.ResolvedEvent;

import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

public interface MergingStrategy<T extends Comparable<T>> {
    T toComparable(ResolvedEvent event);

    final class StreamIndexMergingStrategy implements MergingStrategy<Integer> {
        @Override
        public Integer toComparable(ResolvedEvent event) {
            return 0;
        }
    }

    final class EffectiveTimestampMergingStrategy implements MergingStrategy<Instant> {
        private static final Pattern EFFECTIVE_TIMESTAMP_PATTERN = Pattern.compile("\"effective_timestamp\"\\s*:\\s*\"([^\"]+)\"");

        @Override
        public Instant toComparable(ResolvedEvent event) {
            return effectiveTimestampFrom(event);
        }

        private static Instant effectiveTimestampFrom(ResolvedEvent event) {
            String metadata = new String(event.eventRecord().metadata(), UTF_8);
            Matcher matcher = EFFECTIVE_TIMESTAMP_PATTERN.matcher(metadata);
            if (matcher.find()) {
                return Instant.parse(matcher.group(1));
            }
            return Instant.MIN;
//            throw new IllegalStateException("no timestamp in metadata of " + event);
        }
    }
}
