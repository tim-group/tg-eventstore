package com.timgroup.eventstore.diffing.utils;

import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;

import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class ForkedStreamsLocator {
    // sample body: "oldStream":{"category":"all","id":"all"},"lastWrittenEventNumber":1,
    private static final Pattern STREAM_FORKED_BODY_PATTERN = Pattern.compile("\"category\":\"([^\"]+)\",\"id\":\"([^\"]+)\"},\"lastWrittenEventNumber\":([0-9]+),");

    public static StreamPair<ResolvedEvent> locateForkedStreams(EventSource eventSourceRead1, EventSource eventSource, StreamId forkStreamId) throws Throwable {
        ResolvedEvent streamForkedEvent = eventSourceRead1.readStream().readStreamForwards(forkStreamId).findFirst().orElseThrow(
                (Supplier<Throwable>) () -> new IllegalArgumentException(forkStreamId + " contains no elements")
        );
        if (!"IdempotentStreamForked".equals(streamForkedEvent.eventRecord().eventType())) {
            throw new IllegalStateException(forkStreamId + " does not start with an IdempotentStreamForked event");
        }
        String streamForkedBody = new String(streamForkedEvent.eventRecord().data(), UTF_8);
        Matcher matcher = STREAM_FORKED_BODY_PATTERN.matcher(streamForkedBody);
        if (!matcher.find()) {
            throw new IllegalStateException(streamForkedEvent + " does not contain expected body with oldStream and lastWrittenEventNumber");
        }

        StreamId originalStreamId = StreamId.streamId(matcher.group(1), matcher.group(2));
        long lastWrittenEventNumber = Long.valueOf(matcher.group(3));
        Stream<ResolvedEvent> originalStream = eventSource.readStream().readStreamForwards(originalStreamId, lastWrittenEventNumber);

        return new StreamPair<>(originalStream, eventSource.readStream().readStreamForwards(forkStreamId).skip(1));
    }

    public static final class StreamPair<T> {
        public final Stream<T> originalStream;
        public final Stream<T> forkedStream;

        public StreamPair(Stream<T> originalStream, Stream<T> forkedStream) {
            this.originalStream = originalStream;
            this.forkedStream = forkedStream;
        }
    }
}
