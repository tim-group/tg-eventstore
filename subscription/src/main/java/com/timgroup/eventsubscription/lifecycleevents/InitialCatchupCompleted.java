package com.timgroup.eventsubscription.lifecycleevents;

import com.timgroup.eventstore.api.Position;

import java.util.Objects;

public class InitialCatchupCompleted implements CatchupEvent {
    private final Position position;

    public InitialCatchupCompleted(Position position) {
        this.position = position;
    }

    @Override
    public String toString() {
        return "InitialCatchupCompleted{" +
                "position=" + position +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InitialCatchupCompleted that = (InitialCatchupCompleted) o;
        return Objects.equals(position, that.position);
    }

    @Override
    public int hashCode() {
        return Objects.hash(position);
    }
}
