package com.timgroup.eventstore.readerutils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.timgroup.eventstore.api.*;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.parseLong;
import static java.util.stream.StreamSupport.stream;

public final class RekeyingEventReader implements EventReader {

    private final EventReader underlying;
    private final PositionCodec underlyingPositionCodec;
    private final StreamId newKey;

    private RekeyingEventReader(EventReader underlying, PositionCodec underlyingPositionCodec, StreamId newKey) {
        this.underlying = underlying;
        this.underlyingPositionCodec = underlyingPositionCodec;
        this.newKey = newKey;
    }

    public static RekeyingEventReader rekeying(EventReader underlying, PositionCodec underlyingPositionCodec, StreamId newKey) {
        return new RekeyingEventReader(underlying, underlyingPositionCodec, newKey);
    }

    @Override
    public final Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        RekeyedStreamPosition rekeyedEventPosition = (RekeyedStreamPosition)positionExclusive;

        Stream<ResolvedEvent> events = underlying.readAllForwards(rekeyedEventPosition.underlyingPosition);
        return stream(new RekeyingSpliterator(newKey, rekeyedEventPosition.eventNumber, events.iterator()), false)
                .onClose(events::close);
    }

    @Override
    public final Position emptyStorePosition() {
        return new RekeyedStreamPosition(underlying.emptyStorePosition(), -1L);
    }

    public PositionCodec positionCodec() {
        return new RekeyedStreamPositionCodec(underlyingPositionCodec);
    }

    private static final class RekeyingSpliterator implements Spliterator<ResolvedEvent> {
        private final StreamId newKey;
        private final Iterator<ResolvedEvent> events;

        private long eventNumber;

        public RekeyingSpliterator(StreamId newKey, long lastEventNumber, Iterator<ResolvedEvent> events) {
            this.newKey = newKey;
            this.eventNumber = lastEventNumber;
            this.events = events;
        }

        @Override
        public boolean tryAdvance(Consumer<? super ResolvedEvent> action) {
            if (events.hasNext()) {
                ResolvedEvent event = events.next();
                eventNumber++;
                action.accept(new ResolvedEvent(
                        new RekeyedStreamPosition(event.position(), eventNumber),
                        eventRecord(
                                event.eventRecord().timestamp(),
                                newKey,
                                eventNumber,
                                event.eventRecord().eventType(),
                                event.eventRecord().data(),
                                event.eventRecord().metadata()
                        )
                ));
                return true;
            }
            return false;
        }

        @Override
        public Spliterator<ResolvedEvent> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return MAX_VALUE;
        }

        @Override
        public int characteristics() {
            return ORDERED | NONNULL | DISTINCT;
        }
    }

    private static final class RekeyedStreamPosition implements Position {
        private final Position underlyingPosition;
        private final long eventNumber;

        public RekeyedStreamPosition(Position position, long eventNumber) {
            this.underlyingPosition = position;
            this.eventNumber = eventNumber;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RekeyedStreamPosition that = (RekeyedStreamPosition) o;
            return eventNumber == that.eventNumber &&
                    Objects.equals(underlyingPosition, that.underlyingPosition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(underlyingPosition, eventNumber);
        }

        @Override
        public String toString() {
            return "RekeyedStreamPosition{" +
                    "underlyingPosition=" + underlyingPosition +
                    ", eventNumber=" + eventNumber +
                    '}';
        }
    }

    private static final class RekeyedStreamPositionCodec implements PositionCodec {
        private static final String EVENT_NUMBER_FIELD = "rekeyed_event_number";
        private static final String UNDERLYING_POSITION_FIELD = "underlying_position";

        private final ObjectMapper objectMapper = new ObjectMapper();
        private final PositionCodec underlyingPositionCodec;

        public RekeyedStreamPositionCodec(PositionCodec underlyingPositionCodec) {
            this.underlyingPositionCodec = underlyingPositionCodec;
        }

        @Override
        public String serializePosition(Position position) {
            RekeyedStreamPosition rekeyedPosition = (RekeyedStreamPosition)position;

            Map<String, String> positionMap = new HashMap<>();
            positionMap.put(EVENT_NUMBER_FIELD, String.valueOf(rekeyedPosition.eventNumber));
            positionMap.put(UNDERLYING_POSITION_FIELD, underlyingPositionCodec.serializePosition(rekeyedPosition.underlyingPosition));

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
                long eventNumber = parseLong(positionMap.get(EVENT_NUMBER_FIELD).asText());
                Position deserialisedPosition = underlyingPositionCodec.deserializePosition(positionMap.get(UNDERLYING_POSITION_FIELD).asText());
                return new RekeyedStreamPosition(deserialisedPosition, eventNumber);
            } catch (IOException e) {
                throw new IllegalArgumentException("unable to deserialise position", e);
            }
        }
    }
}
