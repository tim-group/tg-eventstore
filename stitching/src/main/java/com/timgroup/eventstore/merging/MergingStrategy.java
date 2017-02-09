package com.timgroup.eventstore.merging;

import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;

import java.time.Duration;
import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

public interface MergingStrategy<T extends Comparable<T>> {
    StreamId DEFAULT_MERGED_STREAM_ID = StreamId.streamId("all", "all");

    T toComparable(ResolvedEvent event);

    default StreamId mergedStreamId() {
        return DEFAULT_MERGED_STREAM_ID;
    }

    default MergingStrategy<T> withStreamId(StreamId streamId) {
        return new FixedStreamIdMergingStrategy<T>(streamId, this);
    }

    default Duration delay() { return Duration.ZERO; }

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
            throw new IllegalStateException("no timestamp in metadata of " + event);
        }
    }

    final class FixedStreamIdMergingStrategy<T extends Comparable<T>> implements MergingStrategy<T> {
        private final StreamId streamId;
        private final MergingStrategy<T> delegate;

        private FixedStreamIdMergingStrategy(StreamId streamId, MergingStrategy<T> delegate) {
            this.streamId = streamId;
            this.delegate = delegate;
        }

        @Override
        public T toComparable(ResolvedEvent event) {
            return delegate.toComparable(event);
        }

        @Override
        public StreamId mergedStreamId() {
            return streamId;
        }
    }
}
