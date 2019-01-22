package com.timgroup.eventstore.shovelling;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.PeekingIterator;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.readerutils.SingleEventCategoryEventReader;
import com.timgroup.eventstore.writerutils.IdempotentEventStreamWriter;
import com.timgroup.eventstore.writerutils.IdempotentEventStreamWriter.IncompatibleNewEventException;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.component.SimpleValueComponent;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Iterators.partition;
import static com.google.common.collect.Iterators.peekingIterator;
import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.eventstore.writerutils.IdempotentEventStreamWriter.BASIC_COMPATIBILITY_CHECK;
import static com.timgroup.eventstore.writerutils.IdempotentEventStreamWriter.METADATA_COMPATIBILITY_CHECK;
import static com.timgroup.tucker.info.Status.INFO;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public final class EventShovel {
    private static final String SHOVEL_POSITION_METADATA_FIELD = "shovel_position";

    private final int maxBatchSize;
    private final EventReader reader;
    private final PositionCodec readerPositionCodec;
    private final ObjectMapper json = new ObjectMapper();
    private final SimpleValueComponent lastProcessedEvent = new SimpleValueComponent("last-shovelled-event", "Last Shovelled Event");
    private final EventReader outputReader;
    private final EventStreamWriter outputWriter;
    private final String outputCategory;

    public EventShovel(int maxBatchSize, EventReader reader, PositionCodec readerPositionCodec, EventSource output, @Nullable String outputCategory) {
        this.maxBatchSize = maxBatchSize;
        this.reader = requireNonNull(reader);
        this.readerPositionCodec = requireNonNull(readerPositionCodec);
        this.outputCategory = outputCategory;
        if (this.outputCategory != null) {
            this.outputReader = SingleEventCategoryEventReader.curryCategoryReader(output, this.outputCategory);
        } else {
            this.outputReader = output.readAll();
        }
        this.outputWriter = IdempotentEventStreamWriter.idempotent(output.writeStream(), output.readStream(), (a, b) -> {
            BASIC_COMPATIBILITY_CHECK.throwIfIncompatible(a, b);
            if (a.eventRecord().metadata() != b.metadata()) {
                try {
                    ObjectNode aJson = (ObjectNode) readNonNullTree(a.eventRecord().metadata());
                    ObjectNode bJson = (ObjectNode) readNonNullTree(b.metadata());
                    aJson.remove(SHOVEL_POSITION_METADATA_FIELD);
                    bJson.remove(SHOVEL_POSITION_METADATA_FIELD);
                    if (!aJson.equals(bJson)) {
                        METADATA_COMPATIBILITY_CHECK.throwIfIncompatible(a, b);
                    }
                } catch (IOException e) {
                    throw new IncompatibleNewEventException("unable to compare metadata", a, b);
                }
            }
        });
        lastProcessedEvent.updateValue(INFO, "none");
    }

    public EventShovel(int maxBatchSize, EventReader reader, PositionCodec readerPositionCodec, EventSource output) {
        this(maxBatchSize, reader, readerPositionCodec, output, null);
    }

    public EventShovel(EventReader reader, PositionCodec readerPositionCodec, EventSource output) {
        this(reader, readerPositionCodec, output, null);
    }

    public EventShovel(EventReader reader, PositionCodec readerPositionCodec, EventSource output, @Nullable String outputCategory) {
        this(10000, reader, readerPositionCodec, output, outputCategory);
    }

    public void shovelAllNewlyAvailableEvents() {
        Optional<ResolvedEvent> maybeLastWrittenEvent = outputReader.readLastEvent();

        Position currentPosition = maybeLastWrittenEvent
                .map(re -> extractShovelPositionFromMetadata(re.eventRecord().metadata()))
                .orElse(reader.emptyStorePosition());

        try (Stream<ResolvedEvent> resolvedEventStream = reader.readAllForwards(currentPosition)) {
            writeEvents(resolvedEventStream.map(evt -> {
                EventRecord record = evt.eventRecord();
                lastProcessedEvent.updateValue(INFO, record.streamId() + " eventNumber=" + record.eventNumber());
                return new NewEventWithStreamId(
                        redefineStreamId(record.streamId()),
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

    private Position extractShovelPositionFromMetadata(byte[] metadata) {
        try {
            ObjectNode jsonNode = (ObjectNode) json.readTree(new String(metadata, UTF_8));
            return readerPositionCodec.deserializePosition(jsonNode.get(SHOVEL_POSITION_METADATA_FIELD).textValue());
        } catch (Exception e) {
            throw new IllegalStateException("unable to determine current position", e);
        }
    }

    private byte[] createMetadataWithShovelPosition(Position shovelPosition, byte[] upstreamMetadata) {
        try {
            ObjectNode jsonNode = (ObjectNode) readNonNullTree(upstreamMetadata);
            jsonNode.put(SHOVEL_POSITION_METADATA_FIELD, readerPositionCodec.serializePosition(shovelPosition));
            return json.writeValueAsBytes(jsonNode);
        } catch (IOException e) {
            return ("{\"" + SHOVEL_POSITION_METADATA_FIELD + "\":\"" + readerPositionCodec.serializePosition(shovelPosition) + "\"}").getBytes(UTF_8);
        }
    }

    private JsonNode readNonNullTree(byte[] data) throws IOException {
        return Optional.ofNullable(json.readTree(data)).orElseThrow(() -> new IOException("blank json data"));
    }

    private void writeEvents(Stream<NewEventWithStreamId> eventsToWrite) {
        Iterator<Iterator<NewEventWithStreamId>> partitionedByStream = batchBy(
                eventsToWrite.iterator(),
                e -> e.streamId);

        while (partitionedByStream.hasNext()) {
            Iterator<List<NewEventWithStreamId>> batches = partition(partitionedByStream.next(), maxBatchSize);
            while (batches.hasNext()) {
                List<NewEventWithStreamId> batch = batches.next();
                NewEventWithStreamId first = batch.get(0);
                outputWriter.write(first.streamId, transform(batch, e -> e.event), first.eventNumber - 1);
            }
        }
    }

    private StreamId redefineStreamId(StreamId streamId) {
        if (outputCategory != null && !outputCategory.equals(streamId.category())) {
            return StreamId.streamId(outputCategory, streamId.id());
        } else {
            return streamId;
        }
    }

    private static <T> Iterator<Iterator<T>> batchBy(Iterator<T> it, Function<T, Object> grouping) {
        return new Iterator<Iterator<T>>() {
            PeekingIterator<T> peekingIterator = peekingIterator(it);

            @Override
            public boolean hasNext() {
                return peekingIterator.hasNext();
            }

            @Override
            public Iterator<T> next() {
                return new Iterator<T>() {
                    private Object currentGroup = grouping.apply(peekingIterator.peek());

                    private boolean isSameGroup() {
                        return currentGroup.equals(grouping.apply(peekingIterator.peek()));
                    }

                    @Override
                    public boolean hasNext() {
                        return peekingIterator.hasNext() && isSameGroup();
                    }

                    @Override
                    public T next() {
                        if (!isSameGroup()) {
                            throw new NoSuchElementException();
                        }
                        return peekingIterator.next();
                    }
                };
            }
        };
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
