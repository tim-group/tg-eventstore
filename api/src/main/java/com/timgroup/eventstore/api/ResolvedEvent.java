package com.timgroup.eventstore.api;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class ResolvedEvent {
    private final Position position;
    private final EventRecord eventRecord;

    public ResolvedEvent(Position position, EventRecord eventRecord) {
        this.position = requireNonNull(position);
        this.eventRecord = requireNonNull(eventRecord);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResolvedEvent that = (ResolvedEvent) o;
        return Objects.equals(position, that.position) &&
                Objects.equals(eventRecord, that.eventRecord);
    }

    @Override
    public int hashCode() {
        return Objects.hash(position, eventRecord);
    }

    @Override
    public String toString() {
        return "ResolvedEvent{" +
                "position=" + position +
                ", eventRecord=" + eventRecord +
                '}';
    }

    public String locator() {
        return String.format("@%s%s", position, eventRecord.locator());
    }

    public Position position() {
        return position;
    }

    public EventRecord eventRecord() {
        return eventRecord;
    }
}
