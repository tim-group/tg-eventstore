package com.timgroup.eventsubscription.healthcheck;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventsubscription.ChaserListener;
import com.timgroup.eventsubscription.EventProcessorListener;

import java.util.List;
import java.util.Optional;

public class SubscriptionListenerAdapter implements ChaserListener, EventProcessorListener {
    private final Position startingPosition;
    private final List<SubscriptionListener> listeners;

    private volatile Optional<Position> latestFetchedPosition = Optional.empty();
    private volatile Optional<Position> latestProcessedPosition = Optional.empty();

    public SubscriptionListenerAdapter(Position startingPosition, List<SubscriptionListener> listeners) {
        this.startingPosition = startingPosition;
        this.listeners = listeners;
    }

    @Override
    public void transientFailure(Exception e) { }

    @Override
    public void chaserReceived(Position position) {
        latestFetchedPosition = Optional.empty();
        checkStaleness();
    }

    @Override
    public void chaserUpToDate(Position position) {
        latestFetchedPosition = Optional.of(position);
        checkStaleness();
    }

    @Override
    public void eventProcessingFailed(Position position, Exception e) {
        for (SubscriptionListener listener : listeners) {
            listener.terminated(position, e);
        }
    }

    @Override
    public void eventProcessed(Position position) {
        latestProcessedPosition = Optional.of(position);
        checkStaleness();
    }

    @Override
    public void eventDeserializationFailed(Position position, Exception e) {
        for (SubscriptionListener listener : listeners) {
            listener.terminated(position, e);
        }
    }

    @Override
    public void eventDeserialized(Position position) { }

    private void checkStaleness() {
        if (latestFetchedPosition.isPresent() && latestFetchedPosition.equals(latestProcessedPosition)) {
            for (SubscriptionListener listener : listeners) {
                listener.caughtUpAt(latestFetchedPosition.get());
            }
        } else if (latestFetchedPosition.isPresent() && !latestProcessedPosition.isPresent() && latestFetchedPosition.get().equals(startingPosition)) {
            for (SubscriptionListener listener : listeners) {
                listener.caughtUpAt(startingPosition);
            }
        } else if (latestProcessedPosition.isPresent()) {
            for (SubscriptionListener listener : listeners) {
                listener.staleAtVersion(latestProcessedPosition);
            }
        } else {
            for (SubscriptionListener listener : listeners) {
                listener.staleAtVersion(Optional.empty());
            }
        }
    }
}
