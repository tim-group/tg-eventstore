package com.timgroup.eventstore.archiver;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
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
import java.io.IOException;
import java.net.InetAddress;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static com.timgroup.tucker.info.Status.INFO;
import static java.lang.String.format;
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
        this.batchS3ObjectKeyFormat = new S3ArchiveKeyFormat(eventStoreId);
        this.maxPositionFetcher = maxPositionFetcher;
        this.clock = clock;

        this.checkpointPositionComponent =  new SimpleValueComponent(this.monitoringPrefix + "-checkpoint-position",
                "Checkpoint position that archiver resumed from on startup");
        this.checkpointPositionComponent.updateValue(INFO, maxPositionInArchiveOnStartup);

        Map<String, String> appMetadata = new HashMap<>();
        appMetadata.put("event_source", liveEventSource.toString());
        appMetadata.put("app_name", applicationName);
        appMetadata.put("app_version", System.getProperty("timgroup.app.version"));
        appMetadata.put("hostname", hostname());

        this.uncompressedSizeMetrics = metricRegistry.histogram(this.monitoringPrefix + ".archive.batch.uncompressed_size_bytes");
        this.compressedSizeMetrics   = metricRegistry.histogram(this.monitoringPrefix + ".archive.batch.compressed_size_bytes");
        this.batchingUploadHandler = new BatchingUploadHandler(batchingPolicy, output, appMetadata);
        this.eventSubscription = subscriptionBuilder
                .readingFrom(liveEventSource.readAll(), convertPosition(maxPositionInArchiveOnStartup))
                .deserializingUsing(Deserializer.applying(EventRecordHolder::new))
                .publishingTo(batchingUploadHandler)
                .withMaxInitialReplayDuration(Duration.ofMinutes(30))
                .build();

        this.maxPositionInArchive.set(maxPositionInArchiveOnStartup.orElse(0L));
        metricRegistry.gauge(this.monitoringPrefix + ".archive.max_position", () -> maxPositionInArchive::get);
        metricRegistry.gauge(this.monitoringPrefix + ".event_source.max_position", () -> maxPositionInEventSource::get);
        metricRegistry.gauge(this.monitoringPrefix + ".archive.events_awaiting_upload", () -> batchingUploadHandler.s3BatchObjectCreator::eventsInCurrentBatch);
        this.s3ListingTimer = metricRegistry.timer(this.monitoringPrefix + ".archive.list");
        this.s3UploadTimer = metricRegistry.timer(this.monitoringPrefix + ".archive.upload");
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
    public enum RunState { UNSTARTED, RUNNING, STOPPED }

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
        private final Map<String, String> appMetadata;

        private final SimpleValueComponent eventsAwaitingUploadComponent = new SimpleValueComponent(monitoringPrefix + "-events-awaiting-upload", "Number of events awaiting upload to archive");
        private final SimpleValueComponent lastUploadState = new SimpleValueComponent(monitoringPrefix + "-last-upload-state", "Last upload to S3 Archive");
        private final S3BatchObjectCreator s3BatchObjectCreator;

        BatchingUploadHandler(BatchingPolicy batchingPolicy, S3UploadableStorageForInputStream uploadableStorage, Map<String, String> appMetadata) {
            this.batchingPolicy = batchingPolicy;
            this.output = uploadableStorage;
            this.appMetadata = appMetadata;
            this.eventsAwaitingUploadComponent.updateValue(INFO, 0);
            this.lastUploadState.updateValue(INFO, "Nothing uploaded yet");
            this.s3BatchObjectCreator = new S3BatchObjectCreator(
                    S3Archiver.this::positionFrom,
                    batchS3ObjectKeyFormat,
                    uncompressedSizeMetrics,
                    compressedSizeMetrics);
        }

        @Override
        public void apply(@Nonnull Position position, @Nonnull Event deserializedEvent) {
            if (deserializedEvent instanceof EventRecordHolder) {
                ResolvedEvent resolvedEvent = new ResolvedEvent(position, ((EventRecordHolder) deserializedEvent).record);

                s3BatchObjectCreator.add(resolvedEvent);
                batchingPolicy.notifyAddedToBatch(resolvedEvent);

                if (batchingPolicy.ready()) {
                    String key = s3BatchObjectCreator.key();
                    try {
                        S3BatchObject s3BatchObject = s3BatchObjectCreator.prepareBatchForUpload();

                        try (Timer.Context ignored = s3UploadTimer.time()) {
                            Map<String, String> allMetadata = new HashMap<>(appMetadata);
                            allMetadata.putAll(s3BatchObject.metadata);
                            output.upload(key, s3BatchObject.content, s3BatchObject.contentLength, allMetadata);
                        }
                        lastUploadState.updateValue(INFO, format("Successfully uploaded object=[%s] at [%s]", key, clock.instant()));

                        batchingPolicy.reset();
                        s3BatchObjectCreator.reset();
                    } catch (IOException e) {
                        lastUploadState.updateValue(INFO, format("Failed to upload object=[%s] at [%s]%n%s", key, clock.instant(), ComponentUtils.getStackTraceAsString(e)));

                        throw new RuntimeException(
                                format("Error uploading object with key=[%s]%nThrowing exception to halt subscription, and dropping current batch.", key),
                                e);
                    }
                }

                eventsAwaitingUploadComponent.updateValue(INFO, s3BatchObjectCreator.eventsInCurrentBatch());
            }
        }

        @SuppressWarnings("WeakerAccess")
        public Collection<Component> monitoring() {
            return Collections.singleton(eventsAwaitingUploadComponent);
        }

    }

}
