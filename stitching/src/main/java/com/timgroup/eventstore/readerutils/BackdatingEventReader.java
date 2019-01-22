package com.timgroup.eventstore.readerutils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventRecord;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.function.UnaryOperator;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;

public final class BackdatingEventReader extends TransformingEventReader {

    public BackdatingEventReader(EventReader underlying, Instant liveCutoverInclusive) {
        this(underlying, liveCutoverInclusive, Instant.EPOCH);
    }

    public BackdatingEventReader(EventReader underlying, Instant liveCutoverInclusive, Instant destination) {
        super(underlying, toResolvedEventTransformer(new BackdatingTransformer(liveCutoverInclusive, destination)));
    }

    private static final class BackdatingTransformer implements UnaryOperator<EventRecord> {
        private static final String EFFECTIVE_TIMESTAMP = "effective_timestamp";

        private final ObjectMapper json = new ObjectMapper();

        private final Instant liveCutoverInclusive;
        private final Instant destination;

        public BackdatingTransformer(Instant liveCutoverInclusive, Instant destination) {
            this.liveCutoverInclusive = liveCutoverInclusive;
            this.destination = destination;
        }

        @Override
        public EventRecord apply(EventRecord eventRecord) {
            if (effectiveTimestampOf(eventRecord).isBefore(liveCutoverInclusive)) {
                return backdated(eventRecord);
            } else {
                return eventRecord;
            }
        }

        private EventRecord backdated(EventRecord eventRecord) {
            return eventRecord(eventRecord.timestamp(),
                    eventRecord.streamId(),
                    eventRecord.eventNumber(),
                    eventRecord.eventType(),
                    eventRecord.data(),
                    backdateEffectiveTimestamp(eventRecord.metadata()));
        }

        private Instant effectiveTimestampOf(EventRecord eventRecord) {
            try {
                return Instant.parse(readNonNullTree(eventRecord.metadata()).get(EFFECTIVE_TIMESTAMP).asText());
            } catch (IOException | NullPointerException e) {
                throw new IllegalStateException("no effective_timestamp in metadata", e);
            }
        }

        private byte[] backdateEffectiveTimestamp(byte[] upstreamMetadata) {
            try {
                ObjectNode jsonNode = (ObjectNode) readNonNullTree(upstreamMetadata);
                jsonNode.put(EFFECTIVE_TIMESTAMP, destination.toString());
                return json.writeValueAsBytes(jsonNode);
            } catch (IOException e) {
                throw new IllegalStateException("the code should never end up here", e);
            }
        }

        private JsonNode readNonNullTree(byte[] data) throws IOException {
            return Optional.ofNullable(json.readTree(data)).orElseThrow(() -> new IOException("blank json data"));
        }

    }
}
