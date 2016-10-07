package com.timgroup.eventsubscription;

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
    public void chaserReceived(long version) {
        for (ChaserListener listener : listeners) {
            listener.chaserReceived(version);
        }
    }

    @Override
    public void chaserUpToDate(long version) {
        for (ChaserListener listener : listeners) {
            listener.chaserUpToDate(version);
        }
    }
}
