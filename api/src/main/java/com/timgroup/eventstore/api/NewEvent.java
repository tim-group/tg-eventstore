package com.timgroup.eventstore.api;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

public final class NewEvent {
    private static final byte[] EMPTY = new byte[0];
    private final String type;
    private final byte[] data;
    private final byte[] metadata;

    private NewEvent(String type, byte[] data, byte[] metadata) {
        this.type = requireNonNull(type);
        this.data = requireNonNull(data);
        this.metadata = requireNonNull(metadata);
    }

    public static NewEvent newEvent(String type, byte[] data, byte[] metadata) {
        return new NewEvent(type, data, metadata);
    }

    public static NewEvent newEvent(String type, byte[] data) {
        return new NewEvent(type, data, EMPTY);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NewEvent newEvent = (NewEvent) o;

        if (!type.equals(newEvent.type)) return false;
        if (!Arrays.equals(data, newEvent.data)) return false;
        return Arrays.equals(metadata, newEvent.metadata);

    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + Arrays.hashCode(data);
        result = 31 * result + Arrays.hashCode(metadata);
        return result;
    }

    @Override
    public String toString() {
        return "NewEvent{" +
                "type='" + type + '\'' +
                ", data=" + Arrays.toString(data) +
                ", metadata=" + Arrays.toString(metadata) +
                '}';
    }

    public String type() {
        return type;
    }

    public byte[] data() {
        return data;
    }

    public byte[] metadata() {
        return metadata;
    }
}
