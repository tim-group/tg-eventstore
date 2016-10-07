package com.timgroup.eventsubscription.healthcheck;

import com.timgroup.eventsubscription.ChaserListener;
import com.timgroup.eventsubscription.EventProcessorListener;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

public class SubscriptionListenerAdapter implements ChaserListener, EventProcessorListener {
    private final long fromVersion;
    private final List<SubscriptionListener> listeners;

    private volatile Optional<Long> latestFetchedVersion = Optional.empty();
    private volatile Optional<Long> latestProcessedVersion = Optional.empty();

    public SubscriptionListenerAdapter(long fromVersion, List<SubscriptionListener> listeners) {
        this.fromVersion = fromVersion;
        this.listeners = listeners;
    }

    @Override
    public void transientFailure(Exception e) { }

    @Override
    public void chaserReceived(long version) {
        latestFetchedVersion = Optional.empty();
        checkStaleness();
    }

    @Override
    public void chaserUpToDate(long version) {
        latestFetchedVersion = Optional.of(version);
        checkStaleness();
    }

    @Override
    public void eventProcessingFailed(long version, Exception e) {
        for (SubscriptionListener listener : listeners) {
            listener.terminated(version, e);
        }
    }

    @Override
    public void eventProcessed(long version) {
        latestProcessedVersion = Optional.of(version);
        checkStaleness();
    }

    @Override
    public void eventDeserializationFailed(long version, Exception e) {
        for (SubscriptionListener listener : listeners) {
            listener.terminated(version, e);
        }
    }

    @Override
    public void eventDeserialized(long version) { }

    private void checkStaleness() {
        if (latestFetchedVersion.isPresent() && latestFetchedVersion.equals(latestProcessedVersion)) {
            for (SubscriptionListener listener : listeners) {
                listener.caughtUpAt(latestFetchedVersion.get());
            }
        } else if (latestFetchedVersion.isPresent() && !latestProcessedVersion.isPresent() && latestFetchedVersion.get() == fromVersion) {
            for (SubscriptionListener listener : listeners) {
                listener.caughtUpAt(fromVersion);
            }
        } else if (latestProcessedVersion.isPresent()) {
            OptionalLong value = latestProcessedVersion.isPresent() ? OptionalLong.of(latestProcessedVersion.get()) : OptionalLong.empty();

            for (SubscriptionListener listener : listeners) {
                listener.staleAtVersion(value);
            }
        } else {
            for (SubscriptionListener listener : listeners) {
                listener.staleAtVersion(OptionalLong.empty());
            }
        }
    }
}
