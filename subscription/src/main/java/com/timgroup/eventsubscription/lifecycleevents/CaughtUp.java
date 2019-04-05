package com.timgroup.eventsubscription.lifecycleevents;

import com.timgroup.eventstore.api.Position;

import java.time.Instant;
import java.util.Objects;

public class CaughtUp implements CatchupEvent {
    public final Position position;
    private final Instant timestamp;

    public CaughtUp(Position position, Instant timestamp) {
        this.position = position;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "CaughtUp{" +
                "position=" + position +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CaughtUp caughtUp = (CaughtUp) o;
        return Objects.equals(position, caughtUp.position) &&
                Objects.equals(timestamp, caughtUp.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(position, timestamp);
    }

    @Override
    public Position position() {
        return position;
    }

    @Override
    public Instant timestamp() {
        return timestamp;
    }
}
