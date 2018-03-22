package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.Position;

import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public interface ChaserListener {
    void transientFailure(Exception e);

    void chaserReceived(Position position);

    void chaserUpToDate(Position position);
}
