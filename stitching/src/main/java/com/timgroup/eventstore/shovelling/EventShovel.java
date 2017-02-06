package com.timgroup.eventstore.shovelling;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.timgroup.eventstore.api.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class EventShovel {
    private final int maxBatchSize;
    private final EventReader reader;
    private final PositionCodec readerPositionCodec;
    private final EventSource output;
    private final ObjectMapper json = new ObjectMapper();

    public EventShovel(int maxBatchSize, EventReader reader, PositionCodec readerPositionCodec, EventSource output) {
        this.maxBatchSize = maxBatchSize;
        this.reader = reader;
        this.readerPositionCodec = readerPositionCodec;
        this.output = output;
    }

    public EventShovel(EventReader reader, PositionCodec readerPositionCodec, EventSource output) {
        this(10000, reader, readerPositionCodec, output);
    }

    public void shovelAllNewlyAvailableEvents() {
        try (Stream<ResolvedEvent> backwardsWrittenOutputEvents = output.readAll().readAllBackwards()) {
            Optional<ResolvedEvent> maybeLastWrittenEvent = backwardsWrittenOutputEvents.findFirst();

            Position currentPosition = maybeLastWrittenEvent
                    .map(re -> extractShovelPositionFromMetadata(re.eventRecord().metadata()))
                    .orElse(reader.emptyStorePosition());

            try (Stream<ResolvedEvent> resolvedEventStream = reader.readAllForwards(currentPosition)) {
                writeEvents(resolvedEventStream.map(evt -> new NewEventWithStreamId(
                        evt.eventRecord().streamId(),
                        evt.eventRecord().eventNumber(),
                        newEvent(evt.eventRecord().eventType(),
                                evt.eventRecord().data(),
                                createMetadataWithShovelPosition(evt.position(), evt.eventRecord().metadata()))
                )));
            }
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

    private void writeEvents(Stream<NewEventWithStreamId> eventsToWrite) {
        EventStreamWriter eventStreamWriter = output.writeStream();
        StreamId currentStreamId = null;
        long expectedEventNumber = 0L;
        List<NewEvent> batch = new ArrayList<>();

        Iterator<NewEventWithStreamId> events = eventsToWrite.iterator();
        while (events.hasNext()) {
            NewEventWithStreamId next = events.next();
            if (next.streamId.equals(currentStreamId)) {
                batch.add(next.event);
            } else {
                if (!batch.isEmpty()) {
                    eventStreamWriter.write(currentStreamId, batch);
                    batch.clear();
                }

                currentStreamId = next.streamId;
                expectedEventNumber = next.eventNumber - 1L;
                batch.add(next.event);
            }

            if (batch.size() >= maxBatchSize) {
                eventStreamWriter.write(currentStreamId, batch);
                batch.clear();
                currentStreamId = null;
            }
        }

        eventStreamWriter.write(currentStreamId, batch, expectedEventNumber);
    }

    private static final class NewEventWithStreamId {
        private final StreamId streamId;
        private final long eventNumber;
        private final NewEvent event;

        public NewEventWithStreamId(StreamId streamId, long eventNumber, NewEvent event) {
            this.streamId = streamId;
            this.eventNumber = eventNumber;
            this.event = event;
        }
    }

}
