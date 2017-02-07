package com.timgroup.eventstore.merging;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;

import java.util.regex.Pattern;

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
        private static final String SEPARATOR = "###";
        private static final Pattern SEPARATOR_REGEX = Pattern.compile(Pattern.quote(SEPARATOR));

        private final NamedReaderWithCodec[] namedReaders;

        MergedEventReaderPositionCodec(NamedReaderWithCodec... namedReaders) {
            this.namedReaders = namedReaders;
        }

        @Override
        public String serializePosition(Position position) {
            MergedEventReaderPosition mergedPosition = (MergedEventReaderPosition)position;
            StringBuilder serialisedPositionData = new StringBuilder();

            serialisedPositionData
                    .append(mergedPosition.outputEventNumber)
                    .append(SEPARATOR);
            //TODO: serialize out the stream name rather than relying on its index
            for (int index = 0; index < namedReaders.length; index++) {
                serialisedPositionData
                        .append(namedReaders[index].codec.serializePosition(mergedPosition.inputPositions[index]))
                        .append(SEPARATOR);
            }
            return serialisedPositionData.toString();
        }

        @Override
        public Position deserializePosition(String serialisedPosition) {
            String[] serialisedPositionData = SEPARATOR_REGEX.split(serialisedPosition);

            Position[] deserialisedPositions = new Position[namedReaders.length];
            for (int index = 0; index < namedReaders.length; index++) {
                deserialisedPositions[index] = namedReaders[index].codec.deserializePosition(serialisedPositionData[index + 1]);
            }
            return new MergedEventReaderPosition(Long.parseLong(serialisedPositionData[0]), deserialisedPositions);
        }
    }

}
