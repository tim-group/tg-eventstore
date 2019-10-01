package com.timgroup.eventstore.archiver;

import com.codahale.metrics.Histogram;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.zip.GZIPOutputStream;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

class S3BatchObjectCreator {

    private final Function<ResolvedEvent, Long> positionFrom;
    private final S3ArchiveKeyFormat batchS3ObjectKeyFormat;

    private final Histogram uncompressedSizeMetrics;
    private final Histogram compressedSizeMetrics;

    public S3BatchObjectCreator(Function<ResolvedEvent, Long> positionFrom,
                                S3ArchiveKeyFormat batchS3ObjectKeyFormat,
                                Histogram uncompressedSizeMetrics,
                                Histogram compressedSizeMetrics) {
        this.positionFrom = positionFrom;
        this.batchS3ObjectKeyFormat = batchS3ObjectKeyFormat;
        this.uncompressedSizeMetrics = uncompressedSizeMetrics;
        this.compressedSizeMetrics = compressedSizeMetrics;
    }

    public String key(List<ResolvedEvent> batch) {
        ResolvedEvent lastEventInBatch = lastEventInNonEmptyBatch(batch);
        return batchS3ObjectKeyFormat.objectKeyFor(positionFrom.apply(lastEventInBatch), "gz");
    }

    public S3BatchObject create(List<ResolvedEvent> batch) throws IOException {
        try (ByteArrayOutputStream gzippedByteArray = new ByteArrayOutputStream(8192);
             GZIPOutputStream gzipOutputStream = new GZIPOutputStream(gzippedByteArray, 8192)) {

            AtomicInteger uncompressedBytes = new AtomicInteger(0);
            batch.stream()
                .map(this::toProtobufsMessage)
                .map(this::toBytesPrefixedWithLength)
                .forEach(bytes -> {
                    uncompressedBytes.addAndGet(bytes.length);
                    try {
                        gzipOutputStream.write(bytes);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            gzipOutputStream.finish();

            byte[] content =  gzippedByteArray.toByteArray();
            int uncompressedContentSize = uncompressedBytes.get();
            int compressedContentSize = content.length;

            uncompressedSizeMetrics.update(uncompressedContentSize);
            compressedSizeMetrics.update(compressedContentSize);

            return new S3BatchObject(
                    new ByteArrayInputStream(content),
                    compressedContentSize,
                    buildObjectMetadata(uncompressedContentSize, compressedContentSize, batch));
        }
    }


    private byte[] toBytesPrefixedWithLength(Message message) {
        byte[] srcArray = message.toByteArray();
        ByteBuffer buffer= ByteBuffer.allocate(srcArray.length +4).order(LITTLE_ENDIAN);

        buffer.putInt(srcArray.length);
        buffer.put(srcArray);

        return buffer.array();
    }

    private Message toProtobufsMessage(ResolvedEvent resolvedEvent) {
        EventRecord eventRecord = resolvedEvent.eventRecord();
        StreamId streamId = eventRecord.streamId();

        Instant timestamp = eventRecord.timestamp();
        EventStoreArchiverProtos.Timestamp protoTimestamp = EventStoreArchiverProtos.Timestamp.newBuilder()
                .setSeconds(timestamp.getEpochSecond())
                .setNanos(timestamp.getNano())
                .build();

        return EventStoreArchiverProtos.Event.newBuilder()
                .setPosition(positionFrom.apply(resolvedEvent))
                .setTimestamp(protoTimestamp)
                .setStreamCategory(streamId.category())
                .setStreamId(streamId.id())
                .setEventNumber(eventRecord.eventNumber())
                .setEventType(eventRecord.eventType())
                .setData(ByteString.copyFrom(eventRecord.data()))
                .setMetadata(ByteString.copyFrom(eventRecord.metadata()))
                .build();
    }

    private Map<String, String> buildObjectMetadata(int uncompressedContentSize, int compressedContentSize, List<ResolvedEvent> batch) {
        Map<String, String> metadata = new HashMap<>();

        ResolvedEvent maxEvent = lastEventInNonEmptyBatch(batch);
        metadata.put("max_position", String.valueOf(positionFrom.apply(maxEvent)));
        metadata.put("min_position", String.valueOf(positionFrom.apply(batch.get(0))));
        metadata.put("number_of_events_in_batch", String.valueOf(batch.size()));

        EventRecord maxEventRecord = maxEvent.eventRecord();
        metadata.put("max_event_timestamp", maxEventRecord.timestamp().toString());
        metadata.put("max_event_stream_category", maxEventRecord.streamId().category());
        metadata.put("max_event_stream_id", maxEventRecord.streamId().id());
        metadata.put("max_event_event_type", maxEventRecord.eventType());

        metadata.put("uncompressed_size_in_bytes", String.valueOf(uncompressedContentSize));
        metadata.put("compressed_size_in_bytes", String.valueOf(compressedContentSize));
        return metadata;
    }


    private ResolvedEvent lastEventInNonEmptyBatch(List<ResolvedEvent> batch) {
        return batch.get(batch.size() - 1);
    }
}
