package com.timgroup.eventstore.api;

public class ResolvedEvent {
    private final Position position;
    private final EventRecord eventRecord;

    public ResolvedEvent(Position position, EventRecord eventRecord) {
        this.position = position;
        this.eventRecord = eventRecord;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ResolvedEvent that = (ResolvedEvent) o;

        if (!position.equals(that.position)) return false;
        return eventRecord.equals(that.eventRecord);

    }

    @Override
    public int hashCode() {
        int result = position.hashCode();
        result = 31 * result + eventRecord.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ResolvedEvent{" +
                "position=" + position +
                ", eventRecord=" + eventRecord +
                '}';
    }

    public Position position() {
        return position;
    }

    public EventRecord eventRecord() {
        return eventRecord;
    }
}
