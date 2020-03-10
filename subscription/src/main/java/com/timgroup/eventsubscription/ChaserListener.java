package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.Position;

public interface ChaserListener {
    void transientFailure(Exception e);

    void chaserReceived(Position position);

    void chaserUpToDate(Position position);

    void chaserStopped(Position position);
}
