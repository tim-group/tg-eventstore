package com.timgroup.eventstore.archiver;

import com.codahale.metrics.MetricRegistry;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.archiver.monitoring.S3ArchiveBucketConfigurationComponent;
import com.timgroup.eventsubscription.SubscriptionBuilder;
import com.timgroup.remotefilestorage.s3sdkv2.S3SDKv2Storage;
import com.timgroup.tucker.info.Component;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Clock;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static com.timgroup.remotefilestorage.s3sdkv2.S3SDKv2Config.*;
import static java.util.Collections.singletonList;

public class S3ArchiverFactory {

    public static final String CONFIG_EVENTSTORE_S3_ARCHIVE_BUCKETNAME = "tg.eventstore.archive.bucketName";
    public static final String CONFIG_EVENTSTORE_S3_ARCHIVE_OBJECT_PREFIX = "tg.eventstore.archive.object.prefix";
    public static final String S3_ARCHIVER_TMP_DIR_PREFIX = "s3archiver-tmp";

    private final String bucketName;
    private final MetricRegistry metricRegistry;
    private final Clock clock;

    private static final int MAX_KEYS_PER_S3_LISTING = 1_000;
    private final S3Client s3client;
    private final S3SDKv2Storage storage;
    private final List<Component> monitoring;

    public S3ArchiverFactory(String bucketName, Properties config, MetricRegistry metricRegistry, Clock clock) {
        this.bucketName = bucketName;
        this.metricRegistry = metricRegistry;
        this.clock = clock;
        this.s3client = S3Client.builder()
                .applyMutation(addCredentialsFromConfigOrUseDefault(config))
                .applyMutation(addRegionFromConfigOrDefault(config))
                .applyMutation(addProxyFromConfigOrUseDefault(config))
                .build();
        this.storage = S3SDKv2Storage.builder().bucketName(bucketName).client(s3client).build();
        this.monitoring = singletonList(new S3ArchiveBucketConfigurationComponent(s3client, bucketName));
    }

    public S3Archiver newS3Archiver(String eventStoreId, EventSource liveEventSource, int batchsize,  String appName) {
        return build(liveEventSource, new FixedNumberOfEventsBatchingPolicy(batchsize), eventStoreId, appName, "Event", S3Archiver.DEFAULT_MONITORING_PREFIX);
    }

    public S3Archiver newS3Archiver(String eventStoreId, EventSource liveEventSource, BatchingPolicy batchingPolicy, String appName) {
        return build(liveEventSource, batchingPolicy, eventStoreId, appName, eventStoreId + "-Archiver", S3Archiver.DEFAULT_MONITORING_PREFIX + "." + eventStoreId);
    }

    private S3Archiver build(EventSource liveEventSource, BatchingPolicy batchingPolicy, String eventStoreId, String appName, String subscriptionName, String monitoringPrefix) {
        return S3Archiver.newS3Archiver(
                liveEventSource,
                storage,
                eventStoreId,
                SubscriptionBuilder.eventSubscription(subscriptionName),
                batchingPolicy,
                newS3ArchiveMaxPositionFetcher(eventStoreId),
                appName,
                metricRegistry,
                monitoringPrefix,
                clock);
    }

    public Collection<Component> monitoring() {
        return monitoring;
    }

    public EventSource createS3ArchivedEventSource(String eventStoreId) {
        return new S3ArchivedEventSource(storage, storage, bucketName, eventStoreId);
    }

    public S3ArchiveMaxPositionFetcher newS3ArchiveMaxPositionFetcher(String eventStoreId) {
        return new S3ArchiveMaxPositionFetcher(storage, new S3ArchiveKeyFormat(eventStoreId));
    }
}
