package com.timgroup.eventstore.merging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.timgroup.eventstore.api.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.stream.Stream;

import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singleton;

public final class EventShovel {
    private final EventReader reader;
    private final PositionCodec readerPositionCodec;
    private final EventSource output;
    private final ObjectMapper json = new ObjectMapper();

    public EventShovel(EventReader reader, PositionCodec readerPositionCodec, EventSource output) {
        this.reader = reader;
        this.readerPositionCodec = readerPositionCodec;
        this.output = output;
    }

    public void shovelAllNewlyAvailableEvents() {
        try (Stream<ResolvedEvent> backwardsWrittenOutputEvents = output.readAll().readAllBackwards()) {
            Optional<ResolvedEvent> maybeLastWrittenEvent = backwardsWrittenOutputEvents.findFirst();

            Position currentPosition = maybeLastWrittenEvent
                    .map(re -> extractShovelPositionFromMetadata(re.eventRecord().metadata()))
                    .orElse(reader.emptyStorePosition());

            reader.readAllForwards(currentPosition).forEach(evt -> {
                byte[] metadata = createMetadataWithShovelPosition(evt.position(), evt.eventRecord().metadata());
                output.writeStream().write(
                        evt.eventRecord().streamId(),
                        singleton(newEvent(evt.eventRecord().eventType(), evt.eventRecord().data(), metadata))
                );
            });
        }
    }

    private Position extractShovelPositionFromMetadata(byte[] metadata) {
        try {
            ObjectNode jsonNode = (ObjectNode) json.readTree(new String(metadata, UTF_8));
            return readerPositionCodec.deserializePosition(jsonNode.get("shovel_position").textValue());
        } catch (Exception e) {
            throw new IllegalStateException("unable to determine current position", e);
        }
    }

    private byte[] createMetadataWithShovelPosition(Position shovelPosition, byte[] upstreamMetadata) {
        try {
            ObjectNode jsonNode = (ObjectNode) json.readTree(upstreamMetadata);
            jsonNode.put("shovel_position", readerPositionCodec.serializePosition(shovelPosition));
            return json.writeValueAsBytes(jsonNode);
        } catch (IOException e) {
            return ("{\"shovel_position\":\"" + readerPositionCodec.serializePosition(shovelPosition) + "\"}").getBytes(UTF_8);
        }
    }

    //TODO:
    // batched writing
    // optimistic locking

}
