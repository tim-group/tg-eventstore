package com.timgroup.eventstore.diffing;

import com.timgroup.eventstore.api.EventRecord;

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

    private DiffEvent(String effectiveTimestamp, String type, byte[] body) {
        this.effectiveTimestamp = effectiveTimestamp;
        this.type = type;
        this.body = body;
    }

    public static DiffEvent from(EventRecord event) {
        return new DiffEvent(effectiveTimestampOf(event), event.eventType(), event.data());
    }

    public boolean equalsExceptBody(DiffEvent other) {
        return this.effectiveTimestamp.equals(other.effectiveTimestamp)
                && this.type.equals(other.type)
                && !Arrays.equals(this.body, other.body);
    }

    public boolean isEffectiveOnOrBefore(DiffEvent other) {
        return this.effectiveTimestamp.compareTo(other.effectiveTimestamp) <= 0;
    }

    private static String effectiveTimestampOf(EventRecord event) {
        String metadata = new String(event.metadata(), UTF_8);
        Matcher matcher = EFFECTIVE_TIMESTAMP_PATTERN.matcher(metadata);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
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
}
