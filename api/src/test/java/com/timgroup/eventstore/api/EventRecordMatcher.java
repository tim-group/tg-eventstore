package com.timgroup.eventstore.api;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class EventRecordMatcher extends TypeSafeDiagnosingMatcher<EventRecord> {
    private final Instant timestamp;
    private final StreamId streamId;
    private final long eventNumber;
    private final String eventType;
    private final byte[] data;
    private final byte[] metadata;

    public EventRecordMatcher(Instant timestamp, StreamId streamId, long eventNumber, String eventType, byte[] data, byte[] metadata) {
        this.timestamp = timestamp;
        this.streamId = streamId;
        this.eventNumber = eventNumber;
        this.eventType = eventType;
        this.data = data;
        this.metadata = metadata;
    }

    public static EventRecordMatcher anEventRecord(Instant timestamp, StreamId streamId, long eventNumber, String eventType, byte[] data, byte[] metadata) {
        return new EventRecordMatcher(timestamp, streamId, eventNumber, eventType, data, metadata);
    }

    public static EventRecordMatcher anEventRecord(StreamId streamId, long eventNumber, String eventType, String data, String metadata) {
        return new EventRecordMatcher(null, streamId, eventNumber, eventType, data.getBytes(UTF_8), metadata.getBytes(UTF_8));
    }

    @Override
    protected boolean matchesSafely(EventRecord actual, Description mismatchDescription) {
        final EventRecord expected = timestamp == null
                ? eventRecord(actual.timestamp(), streamId, eventNumber, eventType, data, metadata)
                : eventRecord(timestamp, streamId, eventNumber, eventType, data, metadata);

        if (actual.equals(expected)) {
            return true;
        }
        appendDescription(mismatchDescription.appendText("was "), actual.timestamp(), actual.streamId(), actual.eventNumber(), actual.eventType(), actual.data(), actual.metadata());
        return false;
    }

    @Override
    public void describeTo(Description description) {
        appendDescription(description, timestamp, streamId, eventNumber, eventType, data, metadata);
    }

    private void appendDescription(Description description, Instant timestamp, StreamId streamId, long eventNumber, String eventType, byte[] data, byte[] metadata) {
        description.appendText("an EventRecord of type ")
                   .appendText(eventType)
                   .appendText(" at ")
                   .appendText(timestamp == null ? "any time" : timestamp.toString())
                   .appendText("\n        streamId:").appendValue(streamId).appendText(" eventNumber:").appendValue(eventNumber)
                   .appendText("\n        data:").appendText(new String(data, UTF_8))
                   .appendText("\n        metadata:").appendText(new String(metadata, UTF_8))
                   .appendText("\n");
    }
}
