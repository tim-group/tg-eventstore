package com.timgroup.eventsubscription.healthcheck;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;

public class TestPositionCodec implements PositionCodec {
    @Override
    public Position deserializePosition(String string) {
        return new TestPosition(Long.parseLong(string));
    }

    @Override
    public String serializePosition(Position position) {
        return position.toString();
    }
}
