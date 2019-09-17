package com.timgroup.eventstore.archiver;

import com.amazonaws.services.s3.AmazonS3;
import com.codahale.metrics.MetricRegistry;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.archiver.monitoring.S3ArchiveBucketConfigurationComponent;
import com.timgroup.eventsubscription.SubscriptionBuilder;
import com.timgroup.remotefilestorage.s3.S3ClientFactory;
import com.timgroup.remotefilestorage.s3.S3DownloadableStorage;
import com.timgroup.remotefilestorage.s3.S3DownloadableStorageWithoutDestinationFile;
import com.timgroup.remotefilestorage.s3.S3ListableStorage;
import com.timgroup.remotefilestorage.s3.S3UploadableStorage;
import com.timgroup.remotefilestorage.s3.S3UploadableStorageForInputStream;
import com.timgroup.tucker.info.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Clock;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class S3ArchiverFactory {

    public static final String CONFIG_EVENTSTORE_S3_ARCHIVE_BUCKETNAME = "tg.eventstore.archive.bucketName";
    public static final String CONFIG_EVENTSTORE_S3_ARCHIVE_OBJECT_PREFIX = "tg.eventstore.archive.object.prefix";
    public static final String S3_ARCHIVER_TMP_DIR_PREFIX = "s3archiver-tmp";

    private final String eventStoreId;
    private final String bucketName;
    private final MetricRegistry metricRegistry;
    private final Clock clock;
    private final Properties config;

    private static final int MAX_KEYS_PER_S3_LISTING = 1_000;
    private final AmazonS3 amazonS3;
    private List<Component> monitoring;

    @Deprecated
    public S3ArchiverFactory(Properties config, MetricRegistry metricRegistry, Clock clock) {
        this.config = config;
        this.eventStoreId = config.getProperty(CONFIG_EVENTSTORE_S3_ARCHIVE_OBJECT_PREFIX);
        this.bucketName = config.getProperty(CONFIG_EVENTSTORE_S3_ARCHIVE_BUCKETNAME);
        this.metricRegistry = metricRegistry;
        this.clock = clock;
        this.amazonS3 = new S3ClientFactory().fromProperties(config);
        this.monitoring = Collections.singletonList(new S3ArchiveBucketConfigurationComponent(amazonS3, bucketName));
    }

    public S3ArchiverFactory(String bucketName, Properties config, MetricRegistry metricRegistry, Clock clock) {
        this.config = config;
        this.eventStoreId = null;
        this.bucketName = bucketName;
        this.metricRegistry = metricRegistry;
        this.clock = clock;
        this.amazonS3 = new S3ClientFactory().fromProperties(config);
        this.monitoring = Collections.singletonList(new S3ArchiveBucketConfigurationComponent(amazonS3, bucketName));
    }

    @Deprecated
    public S3Archiver newS3Archiver(EventSource liveEventSource, int batchsize,  String appName) {
        return build(liveEventSource, new FixedNumberOfEventsBatchingPolicy(batchsize), this.eventStoreId, appName, "Event", S3Archiver.DEFAULT_MONITORING_PREFIX);
    }

    public S3Archiver newS3Archiver(String eventStoreId, EventSource liveEventSource, int batchsize,  String appName) {
        return build(liveEventSource, new FixedNumberOfEventsBatchingPolicy(batchsize), eventStoreId, appName, "Event", S3Archiver.DEFAULT_MONITORING_PREFIX);
    }

    public S3Archiver newS3Archiver(String eventStoreId, EventSource liveEventSource, BatchingPolicy batchingPolicy, String appName) {
        return build(liveEventSource, batchingPolicy, eventStoreId, appName, eventStoreId + "-Archiver", S3Archiver.DEFAULT_MONITORING_PREFIX + "." + eventStoreId);
    }

    private S3Archiver build(EventSource liveEventSource, BatchingPolicy batchingPolicy, String eventStoreId, String appName, String subscriptionName, String monitoringPrefix) {
        S3UploadableStorageForInputStream s3UploadableStorage = createUploadableStorage();

        return S3Archiver.newS3Archiver(
                liveEventSource,
                s3UploadableStorage,
                eventStoreId,
                SubscriptionBuilder.eventSubscription(subscriptionName),
                batchingPolicy,
                newS3ArchiveMaxPositionFetcher(eventStoreId),
                appName,
                metricRegistry,
                monitoringPrefix,
                clock,
                monitoring);
    }

    public Collection<Component> monitoring() {
        return monitoring;
    }

    public EventSource createS3ArchivedEventSource(String eventStoreId) {
        return new S3ArchivedEventSource(createS3ListableStorage(amazonS3), createDownloadableStorage(amazonS3), eventStoreId);
    }

    private S3DownloadableStorageWithoutDestinationFile createDownloadableStorage(AmazonS3 amazonS3)  {
        try {
            return new S3DownloadableStorageWithoutDestinationFile(
                    new S3DownloadableStorage(amazonS3, Files.createTempDirectory(S3_ARCHIVER_TMP_DIR_PREFIX), bucketName),
                    amazonS3, bucketName);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create tmp dir for Downloadable storage", e);
        }
    }

    public S3ArchiveMaxPositionFetcher newS3ArchiveMaxPositionFetcher(String eventStoreId) {
        final S3ListableStorage s3ListableStorage = createS3ListableStorage(amazonS3);
        return new S3ArchiveMaxPositionFetcher(s3ListableStorage, eventStoreId);
    }

    private S3ListableStorage createS3ListableStorage(AmazonS3 amazonS3) {
        return new S3ListableStorage(amazonS3, bucketName, MAX_KEYS_PER_S3_LISTING);
    }

    private S3UploadableStorageForInputStream createUploadableStorage() {
        return new S3UploadableStorageForInputStream(new S3UploadableStorage(amazonS3, bucketName), amazonS3, bucketName);
    }
}
