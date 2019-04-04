package com.timgroup.eventsubscription.lifecycleevents;

import com.timgroup.eventstore.api.Position;

import java.util.Objects;

public class CaughtUp implements CatchupEvent {
    public final Position position;

    public CaughtUp(Position position) {
        this.position = position;
    }

    @Override
    public String toString() {
        return "CaughtUp{" +
                "position=" + position +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CaughtUp caughtUp = (CaughtUp) o;
        return Objects.equals(position, caughtUp.position);
    }

    @Override
    public int hashCode() {
        return Objects.hash(position);
    }
}
