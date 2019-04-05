package com.timgroup.eventsubscription.lifecycleevents;

import com.timgroup.eventstore.api.Position;

import java.util.Objects;

public class SubscriptionTerminated implements SubscriptionLifecycleEvent {
    public final Position position;
    public final Exception exception;

    public SubscriptionTerminated(Position position, Exception exception) {
        this.position = position;
        this.exception = exception;
    }

    @Override
    public String toString() {
        return "SubscriptionTerminated{" +
                "position=" + position +
                ", exception=" + exception +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubscriptionTerminated that = (SubscriptionTerminated) o;
        return Objects.equals(position, that.position) &&
                Objects.equals(exception, that.exception);
    }

    @Override
    public int hashCode() {
        return Objects.hash(position, exception);
    }
}
