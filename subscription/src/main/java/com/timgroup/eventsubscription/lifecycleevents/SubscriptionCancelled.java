package com.timgroup.eventsubscription.lifecycleevents;

import com.timgroup.eventstore.api.Position;

import java.time.Instant;
import java.util.Objects;

public class SubscriptionCancelled implements SubscriptionLifecycleEvent {
    public final Position position;

    public SubscriptionCancelled(Position position) {
        this.position = position;
    }

    @Override
    public String toString() {
        return "SubscriptionCancelled{" +
                "position=" + position +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubscriptionCancelled that = (SubscriptionCancelled) o;
        return Objects.equals(position, that.position);
    }

    @Override
    public int hashCode() {
        return Objects.hash(position);
    }
}
