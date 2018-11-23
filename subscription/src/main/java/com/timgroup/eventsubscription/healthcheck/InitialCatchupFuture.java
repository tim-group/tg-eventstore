package com.timgroup.eventsubscription.healthcheck;

import com.timgroup.eventstore.api.Position;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public final class InitialCatchupFuture extends CompletableFuture<Position> implements SubscriptionListener {
    @Override
    public void caughtUpAt(Position position) {
        complete(position);
    }

    @Override
    public void staleAtVersion(Optional<Position> position) {
    }

    @Override
    public void terminated(Position position, Exception e) {
        completeExceptionally(new InitialCatchupFailedException(position, e));
    }

    public static final class InitialCatchupFailedException extends CompletionException {
        private final Position position;

        InitialCatchupFailedException(Position position, Throwable cause) {
            super("Initial catchup failed at " + position, cause);
            this.position = position;
        }

        public Position getPosition() {
            return position;
        }
    }
}
