package com.timgroup.eventsubscription.healthcheck;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventsubscription.Event;
import com.timgroup.eventsubscription.EventHandler;
import com.timgroup.eventsubscription.lifecycleevents.InitialCatchupCompleted;
import com.timgroup.eventsubscription.lifecycleevents.SubscriptionTerminated;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public final class InitialCatchupFuture extends CompletableFuture<Position> implements EventHandler {
    private final EventHandler downstream;

    public InitialCatchupFuture() {
        this(EventHandler.DISCARD);
    }

    public InitialCatchupFuture(EventHandler downstream) {
        this.downstream = downstream;
    }

    @Override
    public void apply(Position position, Event deserialized) {
        downstream.apply(position, deserialized);
        if (deserialized instanceof InitialCatchupCompleted) {
            complete(position);
        }
        else if (deserialized instanceof SubscriptionTerminated) {
            SubscriptionTerminated subscriptionTerminated = (SubscriptionTerminated) deserialized;
            completeExceptionally(new InitialCatchupFailedException(subscriptionTerminated.position, subscriptionTerminated.exception));
        }
    }

    public static final class InitialCatchupFailedException extends CompletionException {
        private final Position position;

        public InitialCatchupFailedException(Position position, Throwable cause) {
            super("Initial catchup failed at " + position, cause);
            this.position = position;
        }

        public Position getPosition() {
            return position;
        }
    }
}
