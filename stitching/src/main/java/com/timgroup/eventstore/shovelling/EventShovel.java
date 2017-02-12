package com.timgroup.eventstore.shovelling;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.timgroup.eventstore.api.*;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.component.SimpleValueComponent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Iterators.partition;
import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.indicatorinputstreamwriter.util.IteratorGroupBy.groupBy;
import static com.timgroup.tucker.info.Status.INFO;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;

public final class EventShovel {
    private final int maxBatchSize;
    private final EventReader reader;
    private final PositionCodec readerPositionCodec;
    private final EventSource output;
    private final ObjectMapper json = new ObjectMapper();
    private final SimpleValueComponent lastProcessedEvent = new SimpleValueComponent("last-shovelled-event", "Last Shovelled Event");

    public EventShovel(int maxBatchSize, EventReader reader, PositionCodec readerPositionCodec, EventSource output) {
        this.maxBatchSize = maxBatchSize;
        this.reader = reader;
        this.readerPositionCodec = readerPositionCodec;
        this.output = output;
        lastProcessedEvent.updateValue(INFO, "none");
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
                writeEvents(resolvedEventStream.map(evt -> {
                    EventRecord record = evt.eventRecord();
                    lastProcessedEvent.updateValue(INFO, record.streamId() + " eventNumber=" + record.eventNumber());

                    return new NewEventWithStreamId(
                            record.streamId(),
                            record.eventNumber(),
                            newEvent(
                                    record.eventType(),
                                    record.data(),
                                    createMetadataWithShovelPosition(evt.position(), record.metadata())
                            )
                    );
                }));
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

        Iterator<Iterator<NewEventWithStreamId>> partitionedByStream = groupBy(
                eventsToWrite.iterator(),
                e -> e.streamId);

        while (partitionedByStream.hasNext()) {
            Iterator<List<NewEventWithStreamId>> batches = partition(partitionedByStream.next(), maxBatchSize);
            while (batches.hasNext()) {
                List<NewEventWithStreamId> batch = batches.next();
                NewEventWithStreamId first = batch.get(0);
                eventStreamWriter.write(first.streamId, transform(batch, e -> e.event), first.eventNumber - 1);
            }
        }
    }

    public Iterable<Component> monitoring() {
        return singletonList(lastProcessedEvent);
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
