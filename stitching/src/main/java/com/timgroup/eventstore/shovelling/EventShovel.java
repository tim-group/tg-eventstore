package com.timgroup.eventstore.shovelling;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.PeekingIterator;
import com.timgroup.eventstore.api.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.collect.Iterators.peekingIterator;
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

        Iterator<Iterator<NewEventWithStreamId>> batches = batchBy(
                eventsToWrite.iterator(),
                e -> e.streamId,
                maxBatchSize);

        while (batches.hasNext()) {
            saveBatch(eventStreamWriter, batches.next());
        }
    }

    private void saveBatch(EventStreamWriter eventStreamWriter, Iterator<NewEventWithStreamId> nonEmptyBatch) {
        List<NewEvent> events = new ArrayList<>();
        NewEventWithStreamId first = nonEmptyBatch.next();

        events.add(first.event);
        while (nonEmptyBatch.hasNext()) {
            events.add(nonEmptyBatch.next().event);
        }

        eventStreamWriter.write(first.streamId, events, first.eventNumber - 1);
    }

    private static <T> Iterator<Iterator<T>> batchBy(Iterator<T> it, Function<T, Object> grouping, int maxBatchSize) {
        return new Iterator<Iterator<T>>() {
            PeekingIterator<T> peekingIterator = peekingIterator(it);

            @Override
            public boolean hasNext() {
                return peekingIterator.hasNext();
            }

            @Override
            public Iterator<T> next() {
                return new Iterator<T>() {
                    private int count = 0;
                    private Object currentGroup = grouping.apply(peekingIterator.peek());

                    @Override
                    public boolean hasNext() {
                        return count < maxBatchSize &&
                                peekingIterator.hasNext() &&
                                currentGroup.equals(grouping.apply(peekingIterator.peek()));
                    }

                    @Override
                    public T next() {
                        count += 1;
                        return peekingIterator.next();
                    }
                };
            }
        };
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
