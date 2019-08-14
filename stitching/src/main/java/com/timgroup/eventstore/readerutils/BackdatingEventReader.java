package com.timgroup.eventstore.readerutils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonTokenId;
import com.fasterxml.jackson.core.io.SerializedString;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
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
        private static final JsonFactory JSON = new JsonFactory();

        private final Instant liveCutoverInclusive;
        private final SerializedString destination;

        BackdatingTransformer(Instant liveCutoverInclusive, Instant destination) {
            this.liveCutoverInclusive = liveCutoverInclusive;
            this.destination = new SerializedString(destination.toString());
        }

        @Override
        public EventRecord apply(EventRecord eventRecord) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(eventRecord.metadata().length);
            boolean foundEffectiveTimestamp = false;
            boolean rewroteEffectiveTimestamp = false;

            try (JsonParser parser = JSON.createParser(eventRecord.metadata());
                 JsonGenerator generator = JSON.createGenerator(outputStream)) {

                parser.nextToken();

                if (!parser.isExpectedStartObjectToken())
                    throw new IllegalStateException("Invalid JSON in event metadata (expected object): " + eventRecord);

                generator.writeStartObject();

                String fieldName;
                while ((fieldName = parser.nextFieldName()) != null) {
                    generator.writeFieldName(fieldName);
                    parser.nextToken();
                    //noinspection StringEquality
                    if (fieldName == "effective_timestamp") {
                        foundEffectiveTimestamp = true;
                        if (!parser.hasTokenId(JsonTokenId.ID_STRING))
                            throw new IllegalStateException("Invalid JSON in event metadata (expected string for effective_timestamp): " + eventRecord);
                        Instant effectiveTimestamp = Instant.parse(parser.getText());
                        if (effectiveTimestamp.isBefore(liveCutoverInclusive)) {
                            generator.writeString(destination);
                            rewroteEffectiveTimestamp = true;
                        }
                        else {
                            generator.copyCurrentEvent(parser);
                        }
                    }
                    else {
                        generator.copyCurrentStructure(parser);
                    }
                }

                generator.writeEndObject();
            } catch (IOException e) {
                throw new IllegalStateException("Invalid JSON in event metadata: " + eventRecord, e);
            }

            if (!foundEffectiveTimestamp)
                throw new IllegalStateException("No effective timestamp in event metadata: " + eventRecord);

            if (!rewroteEffectiveTimestamp)
                return eventRecord;

            return eventRecord(eventRecord.timestamp(),
                    eventRecord.streamId(),
                    eventRecord.eventNumber(),
                    eventRecord.eventType(),
                    eventRecord.data(),
                    outputStream.toByteArray());
        }
    }
}
