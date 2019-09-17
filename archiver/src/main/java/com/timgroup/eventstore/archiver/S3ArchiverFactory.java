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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class S3ArchiverFactory {

    public static final String CONFIG_EVENTSTORE_S3_ARCHIVE_BUCKETNAME = "tg.eventstore.archive.bucketName";
    public static final String CONFIG_EVENTSTORE_S3_ARCHIVE_OBJECT_PREFIX = "tg.eventstore.archive.object.prefix";
    public static final String S3_ARCHIVER_TMP_DIR_PREFIX = "s3archiver-tmp";

    private String eventStoreId;
    private String bucketName;
    private MetricRegistry metricRegistry;
    private Clock clock;
    private Properties config;

    private static final int MAX_KEYS_PER_S3_LISTING = 1_000;

    /**
     Configures eventStoreId and bucketName from properties set in config.
     @see #CONFIG_EVENTSTORE_S3_ARCHIVE_BUCKETNAME
     @see #CONFIG_EVENTSTORE_S3_ARCHIVE_OBJECT_PREFIX
     */
    public S3ArchiverFactory(Properties config, MetricRegistry metricRegistry, Clock clock) {
        this.config = config;
        this.eventStoreId = config.getProperty(CONFIG_EVENTSTORE_S3_ARCHIVE_OBJECT_PREFIX);
        this.bucketName = config.getProperty(CONFIG_EVENTSTORE_S3_ARCHIVE_BUCKETNAME);
        this.metricRegistry = metricRegistry;
        this.clock = clock;
    }

    public S3ArchiverFactory(String eventStoreId, String bucketName, Properties config, MetricRegistry metricRegistry, Clock clock) {
        this.config = config;
        this.eventStoreId = eventStoreId;
        this.bucketName = bucketName;
        this.metricRegistry = metricRegistry;
        this.clock = clock;
    }

    public S3Archiver newS3Archiver(EventSource liveEventSource, int batchsize,  String appName) {
        return build(liveEventSource, new FixedNumberOfEventsBatchingPolicy(batchsize), appName, "Event");
    }

    public S3Archiver newS3Archiver(EventSource liveEventSource, BatchingPolicy batchingPolicy, String appName) {
        return build(liveEventSource, batchingPolicy, appName, eventStoreId + "-Archiver");
    }

    private S3Archiver build(EventSource liveEventSource, BatchingPolicy batchingPolicy, String appName, String subscriptionName) {
        AmazonS3 amazonS3 = amazonS3();
        List<Component> monitoring = Collections.singletonList(new S3ArchiveBucketConfigurationComponent(amazonS3, bucketName));
        S3UploadableStorageForInputStream s3UploadableStorage = createUploadableStorage(amazonS3, bucketName);

        return S3Archiver.newS3Archiver(
                liveEventSource,
                s3UploadableStorage,
                eventStoreId,
                SubscriptionBuilder.eventSubscription(subscriptionName),
                batchingPolicy,
                newS3ArchiveMaxPositionFetcher(amazonS3),
                appName,
                metricRegistry,
                clock,
                monitoring);
    }

    public EventSource createS3ArchivedEventSource() {
        AmazonS3 amazonS3 = amazonS3();
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

    public  S3ArchiveMaxPositionFetcher newS3ArchiveMaxPositionFetcher() {
        return newS3ArchiveMaxPositionFetcher(amazonS3());
    }

    public  S3ArchiveMaxPositionFetcher newS3ArchiveMaxPositionFetcher(AmazonS3 amazonS3) {
        final S3ListableStorage s3ListableStorage = createS3ListableStorage(amazonS3);
        return new S3ArchiveMaxPositionFetcher(s3ListableStorage, eventStoreId);
    }

    private S3ListableStorage createS3ListableStorage(AmazonS3 amazonS3) {
        return new S3ListableStorage(amazonS3, bucketName, MAX_KEYS_PER_S3_LISTING);
    }

    private  AmazonS3 amazonS3() {
        return new S3ClientFactory().fromProperties(config);
    }

    private S3UploadableStorageForInputStream createUploadableStorage(AmazonS3 amazonS3, String bucketName) {
        return new S3UploadableStorageForInputStream(new S3UploadableStorage(amazonS3, bucketName), amazonS3, bucketName);
    }
}
