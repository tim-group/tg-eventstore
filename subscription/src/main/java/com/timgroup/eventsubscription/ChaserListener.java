package com.timgroup.eventsubscription;

public interface ChaserListener {
    void transientFailure(Exception e);

    void chaserReceived(long version);

    void chaserUpToDate(long version);
}
