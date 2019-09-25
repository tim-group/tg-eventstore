package com.timgroup.eventstore.archiver;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.codahale.metrics.MetricRegistry;
import com.timgroup.config.ConfigLoader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.archiver.monitoring.S3ArchiveConnectionComponent;
import com.timgroup.eventstore.memory.InMemoryEventSource;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import com.timgroup.eventsubscription.SubscriptionBuilder;
import com.timgroup.eventsubscription.healthcheck.InitialCatchupFuture;
import com.timgroup.remotefilestorage.s3.S3ClientFactory;
import com.timgroup.remotefilestorage.s3.S3DownloadableStorage;
import com.timgroup.remotefilestorage.s3.S3DownloadableStorageWithoutDestinationFile;
import com.timgroup.remotefilestorage.s3.S3ListableStorage;
import com.timgroup.remotefilestorage.s3.S3UploadableStorage;
import com.timgroup.remotefilestorage.s3.S3UploadableStorageForInputStream;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.Report;
import com.timgroup.tucker.info.Status;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Properties;

import static com.amazonaws.SDKGlobalConfiguration.ACCESS_KEY_SYSTEM_PROPERTY;
import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class S3ArchiveEventSourceIntegrationTest extends S3IntegrationTest {
    private AmazonS3 amazonS3;
    private String bucketName;
    private String eventStoreId;
    private String testClassName = getClass().getSimpleName();

    private final Instant fixedEventTimestamp = Instant.parse("2019-03-19T20:43:00.044Z");
    private final Clock fixedClock = Clock.fixed(fixedEventTimestamp, ZoneId.systemDefault());
    private final BatchingPolicy twoEventsPerBatch =  BatchingPolicy.fixedNumberOfEvents(2);
    private final MetricRegistry metricRegistry = new MetricRegistry();

    @Before
    public void
    configure() {
        Properties properties = ConfigLoader.loadConfig(S3_PROPERTIES_FILE);
        amazonS3 = new S3ClientFactory().fromProperties(properties);
        bucketName = properties.getProperty("tg.eventstore.archive.bucketName");
        eventStoreId = uniqueEventStoreId(testClassName);
    }

    @Test public void
    monitoring_includes_component_with_archive_metadata_in_label() throws Exception {
        EventSource s3ArchiveEventSource = createS3ArchivedEventSource();

        S3ArchiveConnectionComponent connectionComponent = getConnectionComponent(s3ArchiveEventSource);
        assertThat(connectionComponent.getLabel(), allOf(containsString(eventStoreId), containsString(bucketName)));
    }

    @Test public void
    monitoring_includes_component_that_is_critical_when_it_cannot_connect_to_s3_archive() throws IOException {
        Properties properties = ConfigLoader.loadConfig(S3_PROPERTIES_FILE);
        properties.setProperty("s3.region", "us-gov-east-1");

        amazonS3 = new S3ClientFactory().fromProperties(properties);
        EventSource s3ArchiveEventSource = createS3ArchivedEventSource();

        Component connectionComponent = getConnectionComponent(s3ArchiveEventSource);

        Report report = connectionComponent.getReport();

        assertThat(report.getStatus(), equalTo(Status.CRITICAL));
        assertThat(report.getValue().toString(), containsString("AmazonS3Exception"));
    }

    @After
    public void clearAccessKeyPropertyOnlyUsedForTestingLackOfAccess() {
        System.clearProperty(ACCESS_KEY_SYSTEM_PROPERTY);
    }

    @Test public void
    monitoring_includes_component_that_is_critical_when_connects_to_s3_archive_but_event_store_does_not_exist() throws IOException {
        EventSource s3ArchiveEventSource = createS3ArchivedEventSource();

        Component connectionComponent = getConnectionComponent(s3ArchiveEventSource);

        Report report = connectionComponent.getReport();

        assertThat(report.getStatus(), equalTo(Status.CRITICAL));
        assertThat(report.getValue().toString(), allOf(
                containsString("Successfully connected to S3 EventStore"),
                containsString("no EventStore with ID='" + eventStoreId + "' exists")));
    }

    @Test public void
    monitoring_includes_component_that_is_okay_and_contains_max_position_when_it_can_connect_to_archive() throws Exception {
        EventSource liveEventSource = new InMemoryEventSource(new JavaInMemoryEventStore(fixedClock));
        StreamId anyStream = streamId(randomCategory(), "1");
        NewEvent anyEvent = newEvent("type-A", randomData(), randomData());
        liveEventSource.writeStream().write(anyStream, asList(anyEvent, anyEvent, anyEvent, anyEvent));
        successfullyArchiveUntilCaughtUp(fixedClock, liveEventSource);

        EventSource s3ArchiveEventSource = createS3ArchivedEventSource();


        Component connectionComponent = getConnectionComponent(s3ArchiveEventSource);

        Report report = connectionComponent.getReport();

        assertThat(report.getStatus(), equalTo(Status.OK));
        assertThat(report.getValue().toString(), allOf(
                containsString("Successfully connected to S3 EventStore"),
                containsString("position=4")));
    }

    private S3ArchiveConnectionComponent getConnectionComponent(EventSource s3ArchiveEventSource) {
        Collection<Component> monitoring = s3ArchiveEventSource.monitoring();
        assertThat(monitoring, hasSize(1));
        Component connectionComponent = monitoring.iterator().next();
        assertThat(connectionComponent, instanceOf(S3ArchiveConnectionComponent.class));
        return (S3ArchiveConnectionComponent) connectionComponent;
    }

    private EventSource createS3ArchivedEventSource() throws IOException {
        return new S3ArchivedEventSource(createListableStorage(), createDownloadableStorage(), bucketName, eventStoreId);
    }

    private S3DownloadableStorageWithoutDestinationFile createDownloadableStorage() throws IOException {
        return new S3DownloadableStorageWithoutDestinationFile(
                new S3DownloadableStorage(amazonS3, Files.createTempDirectory(testClassName), bucketName),
                amazonS3, bucketName);
    }

    private S3ListableStorage createListableStorage() {
        int maxKeysInListingToTriggerPagingBehaviour = 1;
        return new S3ListableStorage(amazonS3, bucketName, maxKeysInListingToTriggerPagingBehaviour);
    }

    private S3Archiver successfullyArchiveUntilCaughtUp(Clock clock, EventSource liveEventSource) {
        InitialCatchupFuture catchupFuture = new InitialCatchupFuture();
        SubscriptionBuilder subscription = SubscriptionBuilder.eventSubscription("test")
                .withRunFrequency(Duration.of(1, ChronoUnit.MILLIS))
                .publishingTo(catchupFuture);

        S3ListableStorage listableStorage = createListableStorage();
        S3Archiver archiver = S3Archiver.newS3Archiver(liveEventSource, createUploadableStorage(), eventStoreId, subscription,
                twoEventsPerBatch, new S3ArchiveMaxPositionFetcher(listableStorage, eventStoreId),
                testClassName, metricRegistry, S3Archiver.DEFAULT_MONITORING_PREFIX, clock);

        archiver.start();

        completeOrFailAfter(catchupFuture, Duration.ofSeconds(5L));

        return archiver;
    }

    private S3UploadableStorageForInputStream createUploadableStorage() {
        return new S3UploadableStorageForInputStream(new S3UploadableStorage(amazonS3, bucketName), amazonS3, bucketName);
    }

}
