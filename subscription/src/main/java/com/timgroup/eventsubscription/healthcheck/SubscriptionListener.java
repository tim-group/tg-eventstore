package com.timgroup.eventsubscription.healthcheck;

import com.timgroup.eventstore.api.Position;

import java.util.Optional;

public interface SubscriptionListener {
    void caughtUpAt(Position position);

    void staleAtVersion(Optional<Position> position);

    void terminated(Position position, Exception e);
}
