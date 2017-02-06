package com.timgroup.eventstore.merging;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;

final class MergedEventReaderPosition implements Position {
    final long outputEventNumber;
    final Position[] inputPositions;

    MergedEventReaderPosition(long outputEventNumber, Position... inputPositions) {
        this.outputEventNumber = outputEventNumber;
        this.inputPositions = inputPositions;
    }

    MergedEventReaderPosition withNextPosition(int readerIndex, Position position) {
        Position[] newPositions = inputPositions.clone();
        newPositions[readerIndex] = position;
        return new MergedEventReaderPosition(outputEventNumber + 1L, newPositions);
    }

    static final class MergedEventReaderPositionCodec implements PositionCodec {
        @Override
        public Position deserializePosition(String string) {
            return new MergedEventReaderPosition(-1L);
        }

        @Override
        public String serializePosition(Position position) {
            return "";
        }
    }

}
