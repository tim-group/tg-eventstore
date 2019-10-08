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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.zip.GZIPOutputStream;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

class CurrentBatchWriter {
    private final BatchingPolicy batchingPolicy;
    private final Function<ResolvedEvent, Long> positionFrom;
    private final S3ArchiveKeyFormat batchS3ObjectKeyFormat;
    private final Histogram uncompressedSizeMetrics;
    private final Histogram compressedSizeMetrics;

    private final AtomicReference<ResolvedEvent> firstEventInBatch = new AtomicReference<>(null);
    private final AtomicReference<ResolvedEvent> lastEventInBatch = new AtomicReference<>(null);
    private final AtomicInteger currentBatchSize = new AtomicInteger(0);
    private final AtomicInteger uncompressedBytesCount = new AtomicInteger(0);
    private final ByteArrayOutputStream gzippedByteArrayOutputStream = new ByteArrayOutputStream(8192);
    private GZIPOutputStream gzipOutputStream;

    public CurrentBatchWriter(BatchingPolicy batchingPolicy,
                              Function<ResolvedEvent, Long> positionFrom,
                              S3ArchiveKeyFormat batchS3ObjectKeyFormat,
                              Histogram uncompressedSizeMetrics,
                              Histogram compressedSizeMetrics) {
        this.batchingPolicy = batchingPolicy;
        this.positionFrom = positionFrom;
        this.batchS3ObjectKeyFormat = batchS3ObjectKeyFormat;
        this.uncompressedSizeMetrics = uncompressedSizeMetrics;
        this.compressedSizeMetrics = compressedSizeMetrics;

        this.reset();
    }

    public void reset() {
        this.batchingPolicy.reset();
        this.firstEventInBatch.set(null);
        this.lastEventInBatch.set(null);
        this.currentBatchSize.set(0);
        this.uncompressedBytesCount.set(0);
        this.gzippedByteArrayOutputStream.reset();
        try {
            this.gzipOutputStream = new GZIPOutputStream(gzippedByteArrayOutputStream, 8192);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void add(ResolvedEvent resolvedEvent) {
        this.firstEventInBatch.compareAndSet(null, resolvedEvent);
        this.lastEventInBatch.set(resolvedEvent);
        this.currentBatchSize.incrementAndGet();

        byte[] uncompressedBytes = Optional.of(resolvedEvent)
                .map(this::toProtobufsMessage)
                .map(this::toBytesPrefixedWithLength)
                .get();

        uncompressedBytesCount.addAndGet(uncompressedBytes.length);

        try {
            gzipOutputStream.write(uncompressedBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.batchingPolicy.notifyAddedToBatch(resolvedEvent);
    }

    public int eventsInCurrentBatch() {
        return currentBatchSize.get();
    }

    public boolean readyToUpload() {
        return batchingPolicy.ready();
    }

    public String key() {
        return batchS3ObjectKeyFormat.objectKeyFor(positionFrom.apply(lastEventInBatch.get()), "gz");
    }

    public S3BatchObject prepareBatchForUpload() throws IOException {
        gzippedByteArrayOutputStream.close();
        gzipOutputStream.finish();

        byte[] content =  gzippedByteArrayOutputStream.toByteArray();
        int uncompressedContentSize = uncompressedBytesCount.get();
        int compressedContentSize = content.length;

        uncompressedSizeMetrics.update(uncompressedContentSize);
        compressedSizeMetrics.update(compressedContentSize);

        return new S3BatchObject(
                new ByteArrayInputStream(content),
                compressedContentSize,
                buildObjectMetadata(uncompressedContentSize, compressedContentSize, lastEventInBatch.get()));
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

    private Map<String, String> buildObjectMetadata(int uncompressedContentSize, int compressedContentSize, ResolvedEvent batch) {
        Map<String, String> metadata = new HashMap<>();

        ResolvedEvent maxEvent = lastEventInBatch.get();
        metadata.put("max_position", String.valueOf(positionFrom.apply(maxEvent)));
        metadata.put("min_position", String.valueOf(positionFrom.apply(firstEventInBatch.get())));
        metadata.put("number_of_events_in_batch", String.valueOf(currentBatchSize.get()));

        EventRecord maxEventRecord = maxEvent.eventRecord();
        metadata.put("max_event_timestamp", maxEventRecord.timestamp().toString());
        metadata.put("max_event_stream_category", maxEventRecord.streamId().category());
        metadata.put("max_event_stream_id", maxEventRecord.streamId().id());
        metadata.put("max_event_event_type", maxEventRecord.eventType());

        metadata.put("uncompressed_size_in_bytes", String.valueOf(uncompressedContentSize));
        metadata.put("compressed_size_in_bytes", String.valueOf(compressedContentSize));
        return metadata;
    }
}
