package com.timgroup.eventstore.merging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Long.parseLong;

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
        private static final String EVENT_NUMBER_FIELD = "merged_event_number";

        private final ObjectMapper objectMapper = new ObjectMapper();

        private final NamedReaderWithCodec[] namedReaders;

        MergedEventReaderPositionCodec(NamedReaderWithCodec... namedReaders) {
            this.namedReaders = namedReaders;
        }

        @Override
        public String serializePosition(Position position) {
            MergedEventReaderPosition mergedPosition = (MergedEventReaderPosition)position;

            Map<String, String> positionMap = new HashMap<>();

            positionMap.put(EVENT_NUMBER_FIELD, String.valueOf(mergedPosition.outputEventNumber));
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

                return new MergedEventReaderPosition(parseLong(positionMap.get(EVENT_NUMBER_FIELD).asText()), deserialisedPositions);

            } catch (IOException e) {
                throw new IllegalArgumentException("unable to deserialise position", e);
            }
        }
    }

}
