package com.timgroup.eventstore.archiver;

import com.amazonaws.util.IOUtils;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.archiver.monitoring.ComponentUtils;
import com.timgroup.eventsubscription.Deserializer;
import com.timgroup.eventsubscription.Event;
import com.timgroup.eventsubscription.EventHandler;
import com.timgroup.eventsubscription.EventSubscription;
import com.timgroup.eventsubscription.SubscriptionBuilder;
import com.timgroup.remotefilestorage.s3.S3UploadableStorageForInputStream;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.Report;
import com.timgroup.tucker.info.Status;
import com.timgroup.tucker.info.component.SimpleValueComponent;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

import static com.timgroup.tucker.info.Status.INFO;
import static java.lang.String.format;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.stream.Collectors.toList;

public class S3Archiver {

    public static final String DEFAULT_MONITORING_PREFIX = "tg-eventstore-s3-archiver";

    private final EventSource liveEventSource;
    private final EventSubscription eventSubscription;

    private final String eventStoreId;
    private final BatchingPolicy batchingPolicy;
    private final S3ArchiveMaxPositionFetcher maxPositionFetcher;
    private final S3ArchiveKeyFormat batchS3ObjectKeyFormat;
    private final BatchingUploadHandler batchingUploadHandler;

    private final Clock clock;

    private final String applicationName;
    private final String applicationHostname;
    private final String applicationVersion;

    private final SimpleValueComponent checkpointPositionComponent;
    private final AtomicLong maxPositionInArchive = new AtomicLong();
    private final AtomicLong maxPositionInEventSource = new AtomicLong();
    private final Timer s3ListingTimer;
    private final Timer s3UploadTimer;
    private final Histogram uncompressedSizeMetrics;
    private final Histogram compressedSizeMetrics;
    private final String monitoringPrefix;

    private RunState runState = RunState.UNSTARTED;

    private S3Archiver(EventSource liveEventSource,
                       S3UploadableStorageForInputStream output,
                       String eventStoreId,
                       SubscriptionBuilder subscriptionBuilder,
                       BatchingPolicy batchingPolicy,
                       Optional<Long> maxPositionInArchiveOnStartup,
                       S3ArchiveMaxPositionFetcher maxPositionFetcher,
                       String applicationName,
                       MetricRegistry metricRegistry,
                       String monitoringPrefix,
                       Clock clock)
    {
        this.liveEventSource = liveEventSource;
        this.eventStoreId = eventStoreId;
        this.monitoringPrefix = monitoringPrefix;
        this.batchingPolicy = batchingPolicy;
        this.applicationName = applicationName;
        this.applicationHostname = hostname();
        this.applicationVersion = System.getProperty("timgroup.app.version");
        this.batchS3ObjectKeyFormat = new S3ArchiveKeyFormat(eventStoreId);
        this.maxPositionFetcher = maxPositionFetcher;
        this.clock = clock;

        this.checkpointPositionComponent =  new SimpleValueComponent(this.monitoringPrefix + "-checkpoint-position",
                "Checkpoint position that archiver resumed from on startup");
        this.checkpointPositionComponent.updateValue(INFO, maxPositionInArchiveOnStartup);

        this.batchingUploadHandler = new BatchingUploadHandler(batchingPolicy, output);
        this.eventSubscription = subscriptionBuilder
                .readingFrom(liveEventSource.readAll(), convertPosition(maxPositionInArchiveOnStartup))
                .deserializingUsing(Deserializer.applying(EventRecordHolder::new))
                .publishingTo(batchingUploadHandler)
                .withMaxInitialReplayDuration(Duration.ofMinutes(30))
                .build();

        this.maxPositionInArchive.set(maxPositionInArchiveOnStartup.orElse(0L));
        metricRegistry.gauge(this.monitoringPrefix + ".archive.max_position", () -> maxPositionInArchive::get);
        metricRegistry.gauge(this.monitoringPrefix + ".event_source.max_position", () -> maxPositionInEventSource::get);
        metricRegistry.gauge(this.monitoringPrefix + ".archive.events_awaiting_upload", () -> batchingUploadHandler.batch::size);
        this.s3ListingTimer = metricRegistry.timer(this.monitoringPrefix + ".archive.list");
        this.s3UploadTimer = metricRegistry.timer(this.monitoringPrefix + ".archive.upload");
        this.uncompressedSizeMetrics = metricRegistry.histogram(this.monitoringPrefix + ".archive.batch.uncompressed_size_bytes");
        this.compressedSizeMetrics   = metricRegistry.histogram(this.monitoringPrefix + ".archive.batch.compressed_size_bytes");
    }

    private static String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "localhost";
        }
    }

    private Position convertPosition(Optional<Long> positionInArchive) {
        Long maxPositionInArchive = positionInArchive.orElse(0L);
        return liveEventSource.readAll().storePositionCodec().deserializePosition(String.valueOf(maxPositionInArchive));
    }

    private Long positionFrom(ResolvedEvent eventFromLiveEventSource) {
        return Long.parseLong(liveEventSource.readAll().storePositionCodec().serializePosition(eventFromLiveEventSource.position()));
    }

    public static S3Archiver newS3Archiver(EventSource liveEventSource, S3UploadableStorageForInputStream output,
            String eventStoreId, SubscriptionBuilder subscriptionBuilder, BatchingPolicy batchingPolicy,
            S3ArchiveMaxPositionFetcher maxPositionFetcher, String applicationName, MetricRegistry metricRegistry,
            String monitoringPrefix, Clock clock) {
        return new S3Archiver(liveEventSource, output, eventStoreId, subscriptionBuilder, batchingPolicy, maxPositionFetcher.maxPosition(),
                maxPositionFetcher, applicationName, metricRegistry, monitoringPrefix, clock);
    }

    public void start() {
        this.eventSubscription.start();
        this.runState = RunState.RUNNING;
    }

    public void stop() {
        this.eventSubscription.stop();
        this.runState = RunState.STOPPED;
    }

    public Optional<ResolvedEvent> lastEventInLiveEventStore() {
        return liveEventSource.readAll().readLastEvent();
    }

    public Collection<Component> monitoring() {
        List<Component> components = new ArrayList<>();
        components.addAll(liveEventSource.monitoring());
        components.addAll(eventSubscription.statusComponents());
        components.add(new ArchiveStalenessComponent());
        components.add(checkpointPositionComponent);
        components.addAll(batchingUploadHandler.monitoring());

        return components.stream().map(c -> c.withStatusNoWorseThan(Status.WARNING)).collect(toList());
    }

    public String getEventStoreId() {
        return eventStoreId;
    }

    public Optional<Long> maxPositionInArchive() {
        try (Timer.Context ignored = this.s3ListingTimer.time()) {
            Optional<Long> maxPosition = this.maxPositionFetcher.maxPosition();
            this.maxPositionInArchive.set(maxPosition.orElse(0L));

            return maxPosition;
        }
    }

    private Optional<Long> maxPositionInLive() {
        Optional<Long> maxPositionInLive = lastEventInLiveEventStore().map(this::positionFrom);
        maxPositionInLive.ifPresent(maxPositionInEventSource::set);
        return maxPositionInLive;
    }

    public ArchiverState state() {
        return new ArchiverState(this.runState, maxPositionInLive(), maxPositionInArchive());
    }
    public enum RunState { UNSTARTED, RUNNING, STOPPED; }

    public static final class EventRecordHolder implements Event {
        @SuppressWarnings("WeakerAccess")
        public final EventRecord record;

        private EventRecordHolder(EventRecord record) {
            this.record = record;
        }
    }

    private final class ArchiveStalenessComponent extends Component {

        ArchiveStalenessComponent() {
            super(monitoringPrefix + "-staleness", "Is archive up to date?");
        }

        @Override
        public Report getReport() {
            Optional<ResolvedEvent> lastEventInLive = lastEventInLiveEventStore();
            Optional<Long> livePosition = lastEventInLive.map(S3Archiver.this::positionFrom);

            Optional<Long> maxPositionInArchive = maxPositionInArchive();
            boolean isStale = batchingPolicy.isStale(maxPositionInArchive, livePosition, lastEventInLive.map(ResolvedEvent::eventRecord));

            String value = format("%s%nmax_position in live=%s%nmax_position in archive=%s",
                    isStale ? "Archive is stale compared to live event store" : "Archive is up to date with live event store",
                    livePosition.map(Object::toString).orElse("[none]"),
                    maxPositionInArchive.map(Object::toString).orElse("[none]")
            );
            return isStale ? new Report(Status.WARNING, value) : new Report(Status.OK, value);
        }
    }

    private final class BatchingUploadHandler implements EventHandler {
        private final BatchingPolicy batchingPolicy;
        private final S3UploadableStorageForInputStream output;

        private final List<ResolvedEvent> batch = new ArrayList<>();
        private final SimpleValueComponent eventsAwaitingUploadComponent = new SimpleValueComponent(monitoringPrefix + "-events-awaiting-upload", "Number of events awaiting upload to archive");
        private final SimpleValueComponent lastUploadState = new SimpleValueComponent(monitoringPrefix + "-last-upload-state", "Last upload to S3 Archive");

        BatchingUploadHandler(BatchingPolicy batchingPolicy, S3UploadableStorageForInputStream output) {
            this.batchingPolicy = batchingPolicy;
            this.output = output;
            this.eventsAwaitingUploadComponent.updateValue(INFO, batch.size());
            this.lastUploadState.updateValue(INFO, "Nothing uploaded yet");
        }

        @Override
        public void apply(@Nonnull Position position, @Nonnull Event deserializedEvent) {
            if (deserializedEvent instanceof EventRecordHolder) {
                batch.add(new ResolvedEvent(position, ((EventRecordHolder) deserializedEvent).record));

                if (batchingPolicy.ready(batch)) {
                    String key = key(batch);
                    try {
                        byte[] uncompressedContent = content(batch);
                        int compressedContentSize;
                        try (Timer.Context ignored = s3UploadTimer.time()) {
                            byte[] compressedContent = applyGzippingToBytes(uncompressedContent);
                            compressedContentSize = compressedContent.length;
                            output.upload(key, new ByteArrayInputStream(compressedContent), compressedContentSize, buildObjectMetadata(uncompressedContent.length, compressedContentSize));
                        }
                        uncompressedSizeMetrics.update(uncompressedContent.length);
                        compressedSizeMetrics.update(compressedContentSize);
                        lastUploadState.updateValue(INFO, format("Successfully uploaded object=[%s] at [%s]", key, clock.instant()));
                        batch.clear();
                    } catch (IOException e) {
                        lastUploadState.updateValue(INFO, format("Failed to upload object=[%s] at [%s]%n%s", key, clock.instant(), ComponentUtils.getStackTraceAsString(e)));

                        throw new RuntimeException(
                                format("Error uploading object with key=[%s]%nThrowing exception to halt subscription, and dropping current batch.", key),
                                e);
                    }
                }

                eventsAwaitingUploadComponent.updateValue(INFO, batch.size());
            }
        }

        private Map<String, String> buildObjectMetadata(int uncompressedContentSize, int compressedContentSize) {
            Map<String, String> metadata = new HashMap<>();

            metadata.put("event_source", liveEventSource.toString());

            metadata.put("app_name", applicationName);
            metadata.put("app_version", applicationVersion);
            metadata.put("hostname", applicationHostname);

            ResolvedEvent maxEvent = lastEventInNonEmptyBatch(batch);
            metadata.put("max_position", String.valueOf(positionFrom(maxEvent)));
            metadata.put("min_position", String.valueOf(positionFrom(batch.get(0))));
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

        private byte[] content(List<ResolvedEvent> batch) {
            return batch
                    .stream()
                    .map(this::toProtobufsMessage)
                    .map(this::toBytesPrefixedWithLength)
                    .collect(
                            ByteArrayOutputStream::new,
                            (outputStream, bytes) -> {
                                try {
                                    outputStream.write(bytes);
                                } catch (IOException e1) {
                                    throw new RuntimeException(e1);
                                }
                            },
                            (a, b) -> { }
                    )
                    .toByteArray();
        }

        private byte[] applyGzippingToBytes(byte[] batchAsProtobufBytes) throws IOException {
            try (ByteArrayOutputStream gzippedByteArray = new ByteArrayOutputStream();
                 GZIPOutputStream gzipOutputStream = new GZIPOutputStream(gzippedByteArray)) {

                IOUtils.copy(new ByteArrayInputStream(batchAsProtobufBytes), gzipOutputStream);
                gzipOutputStream.finish();

                return gzippedByteArray.toByteArray();
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
                    .setPosition(positionFrom(resolvedEvent))
                    .setTimestamp(protoTimestamp)
                    .setStreamCategory(streamId.category())
                    .setStreamId(streamId.id())
                    .setEventNumber(eventRecord.eventNumber())
                    .setEventType(eventRecord.eventType())
                    .setData(ByteString.copyFrom(eventRecord.data()))
                    .setMetadata(ByteString.copyFrom(eventRecord.metadata()))
                    .build();
        }

        private String key(List<ResolvedEvent> batch) {
            ResolvedEvent lastEventInBatch = lastEventInNonEmptyBatch(batch);
            return batchS3ObjectKeyFormat.objectKeyFor(positionFrom(lastEventInBatch), "gz");
        }

        private ResolvedEvent lastEventInNonEmptyBatch(List<ResolvedEvent> batch) {
            return batch.get(batch.size() - 1);
        }

        @SuppressWarnings("WeakerAccess")
        public Collection<Component> monitoring() {
            return Collections.singleton(eventsAwaitingUploadComponent);
        }

    }

}
