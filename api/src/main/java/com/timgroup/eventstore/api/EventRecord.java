package com.timgroup.eventstore.api;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class EventRecord {
    private final Instant timestamp;
    private final StreamId streamId;
    private final long eventNumber;
    private final String eventType;
    private final byte[] data;
    private final byte[] metadata;

    private EventRecord(Instant timestamp, StreamId streamId, long eventNumber, String eventType, byte[] data, byte[] metadata) {
        this.timestamp = requireNonNull(timestamp);
        this.streamId = requireNonNull(streamId);
        this.eventNumber = eventNumber;
        this.eventType = requireNonNull(eventType);
        this.data = requireNonNull(data);
        this.metadata = requireNonNull(metadata);
    }

    public static EventRecord eventRecord(Instant timestamp, StreamId streamId, long eventNumber, String eventType, byte[] data, byte[] metadata) {
        return new EventRecord(timestamp, streamId, eventNumber, eventType, data, metadata);
    }

    public ResolvedEvent toResolvedEvent(Position position) {
        return new ResolvedEvent(position, this);
    }

    @Nonnull
    public String locator() {
        return String.format("<%s/%s/%s>(%s)",
                streamId().category(),
                streamId().id(),
                eventNumber(),
                eventType()
        );
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventRecord that = (EventRecord) o;
        return eventNumber == that.eventNumber &&
                Objects.equals(timestamp, that.timestamp) &&
                Objects.equals(streamId, that.streamId) &&
                Objects.equals(eventType, that.eventType) &&
                Arrays.equals(data, that.data) &&
                Arrays.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, streamId, eventNumber, eventType, data, metadata);
    }

    @Override
    public String toString() {
        return "EventRecord{" +
                "timestamp=" + timestamp +
                ", streamId=" + streamId +
                ", eventNumber=" + eventNumber +
                ", eventType='" + eventType + '\'' +
                ", data=" + byteArrayAsString(data) +
                ", metadata=" + byteArrayAsString(metadata) +
                '}';
    }

    private static String byteArrayAsString(byte[] data) {
        try {
            return new String(data, StandardCharsets.UTF_8);
        } catch (Throwable t) {
            return Arrays.toString(data);
        }
    }

    public long eventNumber() {
        return eventNumber;
    }

    @Nonnull
    public StreamId streamId() {
        return streamId;
    }

    @Nonnull
    public String eventType() {
        return eventType;
    }

    @Nonnull
    public Instant timestamp() {
        return timestamp;
    }

    @Nonnull
    public byte[] data() {
        return data;
    }

    @Nonnull
    public byte[] metadata() {
        return metadata;
    }
}
