package com.timgroup.eventstore.api;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class ResolvedEventMatcher extends TypeSafeDiagnosingMatcher<ResolvedEvent> {

    private final ResolvedEvent expected;

    public static ResolvedEventMatcher aResolvedEvent(Position position, EventRecord eventRecord) {
        return new ResolvedEventMatcher(new ResolvedEvent(position, eventRecord));
    }

    private ResolvedEventMatcher(ResolvedEvent expected) {
        this.expected = expected;
    }

    @Override
    protected boolean matchesSafely(ResolvedEvent actual, Description mismatchDescription) {
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

    private void appendDescription(Description description, ResolvedEvent resolvedEvent) {
        EventRecord record = resolvedEvent.eventRecord();
        description.appendText("a ResolvedEvent at position ")
                   .appendText(resolvedEvent.position().toString())
                   .appendText(" of type ")
                   .appendText(record.eventType())
                   .appendText(" at ")
                   .appendText(record.timestamp().toString())
                   .appendText("\n        streamId:").appendValue(record.streamId()).appendText(" eventNumber:").appendValue(record.eventNumber())
                   .appendText("\n        data:").appendText(new String(record.data(), UTF_8))
                   .appendText("\n        metadata:").appendText(new String(record.metadata(), UTF_8))
                   .appendText("\n");
    }
}
