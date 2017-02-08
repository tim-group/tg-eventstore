package com.timgroup.eventstore.readerutils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.io.IOException;
import java.time.Instant;
import java.util.stream.Stream;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;

public class BackdatingEventReader implements EventReader {
    private final EventReader underlying;
    private final Instant liveCutoverInclusive;
    private final ObjectMapper json = new ObjectMapper();
    private String EFFECTIVE_TIMESTAMP = "effective_timestamp";


    public BackdatingEventReader(EventReader underlying, Instant liveCutoverInclusive) {
        this.underlying = underlying;
        this.liveCutoverInclusive = liveCutoverInclusive;
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        return underlying.readAllForwards(positionExclusive).map(this::possiblyBackdate);
    }

    private ResolvedEvent possiblyBackdate(ResolvedEvent resolvedEvent) {
        if (effectiveTimestampOf(resolvedEvent.eventRecord()).isBefore(liveCutoverInclusive)) {
            EventRecord eventRecord = resolvedEvent.eventRecord();
            return new ResolvedEvent(resolvedEvent.position(),
                    eventRecord(eventRecord.timestamp(),
                            eventRecord.streamId(),
                            eventRecord.eventNumber(),
                            eventRecord.eventType(),
                            eventRecord.data(),
                            backdateEffectiveTimestamp(eventRecord.metadata())));
        } else {
            return resolvedEvent;
        }
    }

    private Instant effectiveTimestampOf(EventRecord eventRecord) {
        try {
            return Instant.parse(json.readTree(eventRecord.metadata()).get(EFFECTIVE_TIMESTAMP).asText());
        } catch (IOException e) {
            return null;
        }
    }

    private byte[] backdateEffectiveTimestamp(byte[] upstreamMetadata) {
        try {
            ObjectNode jsonNode = (ObjectNode) json.readTree(upstreamMetadata);
            jsonNode.put(EFFECTIVE_TIMESTAMP, Instant.EPOCH.toString());
            return json.writeValueAsBytes(jsonNode);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public Position emptyStorePosition() {
        return underlying.emptyStorePosition();
    }

    @Override
    public Stream<ResolvedEvent> readAllBackwards() {
        return underlying.readAllBackwards().map(this::possiblyBackdate);
    }

    @Override
    public Stream<ResolvedEvent> readAllBackwards(Position positionExclusive) {
        return underlying.readAllBackwards(positionExclusive).map(this::possiblyBackdate);
    }
}
