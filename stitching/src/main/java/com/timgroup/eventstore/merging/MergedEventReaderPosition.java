package com.timgroup.eventstore.merging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

final class MergedEventReaderPosition implements Position {
    final Position[] inputPositions;

    MergedEventReaderPosition(Position... inputPositions) {
        this.inputPositions = inputPositions;
    }

    MergedEventReaderPosition withNextPosition(int readerIndex, Position position) {
        Position[] newPositions = inputPositions.clone();
        newPositions[readerIndex] = position;
        return new MergedEventReaderPosition(newPositions);
    }

    @Override
    public String toString() {
        return Arrays.toString(inputPositions);
    }

    static final class MergedEventReaderPositionCodec implements PositionCodec {
        private final ObjectMapper objectMapper = new ObjectMapper();

        private final NamedReaderWithCodec[] namedReaders;

        MergedEventReaderPositionCodec(NamedReaderWithCodec... namedReaders) {
            this.namedReaders = namedReaders;
        }

        @Override
        public String serializePosition(Position position) {
            MergedEventReaderPosition mergedPosition = (MergedEventReaderPosition) position;

            Map<String, String> positionMap = new HashMap<>();

            for (int index = 0; index < namedReaders.length; index++) {
                positionMap.put(namedReaders[index].name, namedReaders[index].codec.serializePosition(mergedPosition.inputPositions[index]));
            }

            try {
                return objectMapper.writeValueAsString(positionMap);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("unable to serialise position", e);
            }
        }

        @Override
        public Position deserializePosition(String serialisedPosition) {
            try {
                JsonNode positionMap = objectMapper.readTree(serialisedPosition);

                Position[] deserialisedPositions = new Position[namedReaders.length];
                for (int index = 0; index < namedReaders.length; index++) {
                    deserialisedPositions[index] = namedReaders[index].codec.deserializePosition(positionMap.get(namedReaders[index].name).asText());
                }

                return new MergedEventReaderPosition(deserialisedPositions);

            } catch (IOException e) {
                throw new IllegalArgumentException("unable to deserialise position", e);
            }
        }
    }

}
