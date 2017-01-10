package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;

public class BasicMysqlPositionCodec implements PositionCodec {
    @Override
    public Position deserializePosition(String string) {
        return new BasicMysqlEventStorePosition(Long.parseLong(string));
    }

    @Override
    public String serializePosition(Position position) {
        return Long.toString(((BasicMysqlEventStorePosition) position).value);
    }

    @Override
    public int comparePositions(Position left, Position right) {
        long leftValue = ((BasicMysqlEventStorePosition) left).value;
        long rightValue = ((BasicMysqlEventStorePosition) right).value;
        return Long.compare(leftValue, rightValue);
    }
}
