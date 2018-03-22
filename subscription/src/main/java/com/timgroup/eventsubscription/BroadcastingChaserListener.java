package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.Position;

import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public class BroadcastingChaserListener implements ChaserListener {
    private final ChaserListener[] listeners;

    public BroadcastingChaserListener(ChaserListener... listeners) {
        this.listeners = listeners;
    }

    @Override
    public void transientFailure(Exception e) {
        for (ChaserListener listener : listeners) {
            listener.transientFailure(e);
        }
    }

    @Override
    public void chaserReceived(Position position) {
        for (ChaserListener listener : listeners) {
            listener.chaserReceived(position);
        }
    }

    @Override
    public void chaserUpToDate(Position position) {
        for (ChaserListener listener : listeners) {
            listener.chaserUpToDate(position);
        }
    }
}
