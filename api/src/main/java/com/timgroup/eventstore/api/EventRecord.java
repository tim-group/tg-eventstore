package com.timgroup.eventstore.api;

import java.time.Instant;
import java.util.Arrays;

public class EventRecord {
    private final Instant timestamp;
    private final StreamId streamId;
    private final int eventNumber;
    private final String eventType;
    private final byte[] data;
    private final byte[] metadata;

    private EventRecord(Instant timestamp, StreamId streamId, int eventNumber, String eventType, byte[] data, byte[] metadata) {
            this.timestamp = timestamp;
            this.streamId = streamId;
            this.eventNumber = eventNumber;
            this.eventType = eventType;
            this.data = data;
            this.metadata = metadata;
    }

    public static EventRecord eventRecord(Instant timestamp, StreamId streamId, int eventNumber, String eventType, byte[] data, byte[] metadata) {
        return new EventRecord(timestamp, streamId, eventNumber, eventType, data, metadata);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EventRecord that = (EventRecord) o;

        if (eventNumber != that.eventNumber) return false;
        if (!timestamp.equals(that.timestamp)) return false;
        if (!streamId.equals(that.streamId)) return false;
        if (!eventType.equals(that.eventType)) return false;
        if (!Arrays.equals(data, that.data)) return false;
        return Arrays.equals(metadata, that.metadata);

    }

    @Override
    public int hashCode() {
        int result = timestamp.hashCode();
        result = 31 * result + streamId.hashCode();
        result = 31 * result + (int) (eventNumber ^ (eventNumber >>> 32));
        result = 31 * result + eventType.hashCode();
        result = 31 * result + Arrays.hashCode(data);
        result = 31 * result + Arrays.hashCode(metadata);
        return result;
    }

    @Override
    public String toString() {
        return "EventRecord{" +
                "timestamp=" + timestamp +
                ", streamId='" + streamId + '\'' +
                ", eventNumber=" + eventNumber +
                ", eventType='" + eventType + '\'' +
                ", data=" + Arrays.toString(data) +
                ", metadata=" + Arrays.toString(metadata) +
                '}';
    }

    public int eventNumber() {
        return eventNumber;
    }

    public StreamId streamId() {
        return streamId;
    }

    public String eventType() {
        return eventType;
    }

    public Instant timestamp() {
        return timestamp;
    }

    public byte[] data() {
        return data;
    }

    public byte[] metadata() {
        return metadata;
    }
}
