package com.timgroup.eventsubscription.lifecycleevents;

import com.timgroup.eventstore.api.Position;

import java.time.Instant;
import java.util.Objects;

public class InitialCatchupCompleted implements CatchupEvent {
    private final Position position;
    private final Instant timestamp;

    public InitialCatchupCompleted(Position position, Instant timestamp) {
        this.position = position;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "InitialCatchupCompleted{" +
                "position=" + position +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InitialCatchupCompleted that = (InitialCatchupCompleted) o;
        return Objects.equals(position, that.position) &&
                Objects.equals(timestamp, that.timestamp);
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
