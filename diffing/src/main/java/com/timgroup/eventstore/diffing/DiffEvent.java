package com.timgroup.eventstore.diffing;

import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

final class DiffEvent {
    private static final Pattern EFFECTIVE_TIMESTAMP_PATTERN = Pattern.compile("\"effective_timestamp\"\\s*:\\s*\"([^\"]+)\"");

    public final String effectiveTimestamp;
    public final String type;
    public final byte[] body;
    public final ResolvedEvent underlyingEvent;

    DiffEvent(String effectiveTimestamp, String type, byte[] body, ResolvedEvent underlyingEvent) {
        this.effectiveTimestamp = effectiveTimestamp;
        this.type = type;
        this.body = body;
        this.underlyingEvent = underlyingEvent;
    }

    public static DiffEvent from(ResolvedEvent event) {
        EventRecord eventRecord = event.eventRecord();
        return new DiffEvent(effectiveTimestampOf(eventRecord), eventRecord.eventType(), eventRecord.data(), event);
    }

    public boolean isSimilarTo(DiffEvent other) {
        int numberOfEqualProperties = (this.effectiveTimestamp.equals(other.effectiveTimestamp) ? 1 : 0)
                + (this.type.equals(other.type) ? 1 : 0)
                + (Arrays.equals(this.body, other.body) ? 1 : 0);
        return numberOfEqualProperties == 2;
    }

    public boolean isEffectiveOnOrBefore(DiffEvent other) {
        return this.effectiveTimestamp.compareTo(other.effectiveTimestamp) <= 0;
    }

    private static String effectiveTimestampOf(EventRecord event) {
        String metadata = new String(event.metadata(), UTF_8);
        Matcher matcher = EFFECTIVE_TIMESTAMP_PATTERN.matcher(metadata);
        return matcher.find() ? matcher.group(1) : "";
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DiffEvent that = (DiffEvent) o;
        return Objects.equals(effectiveTimestamp, that.effectiveTimestamp) &&
                Objects.equals(type, that.type) &&
                Arrays.equals(body, that.body);
    }

    @Override public int hashCode() {
        return Objects.hash(effectiveTimestamp, type, body);
    }

    @Override public String toString() { return "DiffEvent{underlyingEvent=" + underlyingEvent + '}'; }
}
