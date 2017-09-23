package com.timgroup.eventstore.api;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.time.Instant;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class EventRecordMatcher extends TypeSafeDiagnosingMatcher<EventRecord> {

    private final EventRecord expected;

    public static EventRecordMatcher anEventRecord(Instant timestamp, StreamId streamId, long eventNumber, String eventType, byte[] data, byte[] metadata) {
        return new EventRecordMatcher(eventRecord(timestamp, streamId, eventNumber, eventType, data, metadata));
    }

    private EventRecordMatcher(EventRecord expected) {
        this.expected = expected;
    }

    @Override
    protected boolean matchesSafely(EventRecord actual, Description mismatchDescription) {
        if (actual.equals(expected)) {
            return true;
        }

        appendDescription(mismatchDescription.appendText("was "), actual);

        return false;
    }

    @Override
    public void describeTo(Description description) {
        appendDescription(description, expected);
    }

    private void appendDescription(Description description, EventRecord record) {
        description.appendText("an EventRecord of type ")
                   .appendText(record.eventType())
                   .appendText(" at ")
                   .appendText(record.timestamp().toString())
                   .appendText("\n        streamId:").appendValue(record.streamId()).appendText(" eventNumber:").appendValue(record.eventNumber())
                   .appendText("\n        data:").appendText(new String(record.data(), UTF_8))
                   .appendText("\n        metadata:").appendText(new String(record.metadata(), UTF_8))
                   .appendText("\n");
    }
}
