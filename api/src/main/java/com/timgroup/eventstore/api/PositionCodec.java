package com.timgroup.eventstore.api;

public interface PositionCodec {
    Position deserializePosition(String string);

    String serializePosition(Position position);
}
