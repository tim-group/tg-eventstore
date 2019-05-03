package com.timgroup.eventstore.archiver;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import com.codahale.metrics.MetricRegistry;
import com.timgroup.config.ConfigLoader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.archiver.monitoring.S3ArchiveBucketConfigurationComponent;
import com.timgroup.eventstore.memory.InMemoryEventSource;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import com.timgroup.eventsubscription.SubscriptionBuilder;
import com.timgroup.eventsubscription.healthcheck.InitialCatchupFuture;
import com.timgroup.remotefilestorage.api.ListableStorage;
import com.timgroup.remotefilestorage.s3.S3ClientFactory;
import com.timgroup.remotefilestorage.s3.S3DownloadableStorage;
import com.timgroup.remotefilestorage.s3.S3DownloadableStorageWithoutDestinationFile;
import com.timgroup.remotefilestorage.s3.S3ListableStorage;
import com.timgroup.remotefilestorage.s3.S3UploadableStorage;
import com.timgroup.remotefilestorage.s3.S3UploadableStorageForInputStream;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.Report;
import com.timgroup.tucker.info.Status;
import com.youdevise.testutils.matchers.Contains;
import net.ttsui.junit.rules.pending.PendingRule;
import org.apache.commons.lang3.RandomStringUtils;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.zip.GZIPInputStream;

import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.eventstore.api.ObjectPropertiesMatcher.objectWith;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.doReturn;


//TO run the test, please follow instructions in README.md in this module.
public class S3ArchiverIntegrationTest {

    public static final String S3_PROPERTIES_FILE = "s3_do_not_check_in.properties";
    private AmazonS3 amazonS3;
    private String bucketName;
    private String eventStoreId;
    private String testName = getClass().getSimpleName();
    private final Long descendingCounter = Long.MAX_VALUE - System.currentTimeMillis();

    private final String category_1 = randomCategory();
    private final StreamId stream_1 = streamId(category_1, "1");
    private final NewEvent event_1 = newEvent("type-A", randomData(), randomData());
    private final NewEvent event_2 = newEvent("type-B", randomData(), randomData());
    private final NewEvent event_3 = newEvent("type-C", randomData(), randomData());
    private final NewEvent event_4 = newEvent("type-D", randomData(), randomData());
    private final NewEvent event_5 = newEvent("type-E", randomData(), randomData());

    private final Instant fixedEventTimestamp = Instant.parse("2019-03-19T20:43:00.044Z");
    private final Clock fixedClock = Clock.fixed(fixedEventTimestamp, ZoneId.systemDefault());
    private final BatchingPolicy twoEventsPerBatch =  BatchingPolicy.fixedNumberOfEvents(2);
    private final MetricRegistry metricRegistry = new MetricRegistry();

    @Rule public PendingRule pendingRule = new PendingRule();
    @Rule public TestName testNameRule = new TestName();
    private int uncompressed_batch_size_limit_bytes = 10_000;


    @BeforeClass
    public static void verifyS3CredentialsSupplied() {
        assumeTrue("S3 credentials must be supplied via properties file", Files.exists(Paths.get(S3_PROPERTIES_FILE)));
    }

    @Before public void
    configure() {
        Properties properties = ConfigLoader.loadConfig(S3_PROPERTIES_FILE);
        amazonS3 = new S3ClientFactory().fromProperties(properties);
        bucketName = properties.getProperty("tg.eventstore.archive.bucketName");
        eventStoreId = "test-eventstore-" + descendingCounter + "-" + testName + "." + testNameRule.getMethodName() + "-" + RandomStringUtils.randomAlphabetic(10);
    }

    @Test public void
    readLastEvent_is_empty_when_there_are_no_events() throws IOException {
        EventSource s3EventSource = createS3ArchivedEventSource();
        assertThat(s3EventSource.readAll().readLastEvent(), equalTo(Optional.empty()));
    }

    @Test public void
    readAllForwards_is_empty_when_there_are_no_events() throws IOException {
        EventSource s3EventSource = createS3ArchivedEventSource();
        assertThat(s3EventSource.readAll().readAllForwards().count(), equalTo(0L));
    }


    @Test public void
    readAllForwards_can_reconstitute_events() throws Exception {
        EventSource liveEventSource = new InMemoryEventSource(new JavaInMemoryEventStore(fixedClock));

        liveEventSource.writeStream().write(stream_1, asList(event_1, event_2));

        successfullyArchiveUntilCaughtUp(liveEventSource);

        EventSource s3EventSource = createS3ArchivedEventSource();

        List<EventRecord> resolvedEvents = s3EventSource
                .readAll()
                .readAllForwards()
                .map(ResolvedEvent::eventRecord)
                .collect(toList());

        assertThat(resolvedEvents, Contains.inOrder(
                objectWith(EventRecord::streamId, stream_1)
                        .and(EventRecord::eventType, event_1.type())
                        .and(EventRecord::data, event_1.data())
                        .and(EventRecord::metadata, event_1.metadata())
                        .and(EventRecord::timestamp, fixedEventTimestamp),
                objectWith(EventRecord::streamId, stream_1)
                        .and(EventRecord::eventNumber, 1L)
                        .and(EventRecord::eventType, event_2.type())
                        .and(EventRecord::data, event_2.data())
                        .and(EventRecord::metadata, event_2.metadata())
                        .and(EventRecord::timestamp, fixedEventTimestamp)
        ));
    }

    @Test public void
    readAllForwards_consumes_all_available_batches() throws Exception {
        EventSource liveEventSource = new InMemoryEventSource(new JavaInMemoryEventStore(fixedClock));

        liveEventSource.writeStream().write(stream_1, asList(event_1, event_2, event_3, event_4, event_5));

        successfullyArchiveUntilCaughtUp(liveEventSource);

        EventSource s3EventSource = createS3ArchivedEventSource();

        List<EventRecord> resolvedEvents = s3EventSource
                .readAll()
                .readAllForwards()
                .map(ResolvedEvent::eventRecord)
                .collect(toList());

        assertThat(resolvedEvents, Contains.inOrder(
                objectWith(EventRecord::streamId, stream_1).and(EventRecord::eventType, event_1.type()),
                objectWith(EventRecord::streamId, stream_1).and(EventRecord::eventType, event_2.type()),
                objectWith(EventRecord::streamId, stream_1).and(EventRecord::eventType, event_3.type()),
                objectWith(EventRecord::streamId, stream_1).and(EventRecord::eventType, event_4.type())
        ));
    }

    @Test public void
    readLastEvent_provides_last_event_from_last_batch_in_s3_bucket() throws Exception {
        EventSource liveEventSource = new InMemoryEventSource(new JavaInMemoryEventStore(fixedClock));

        liveEventSource.writeStream().write(stream_1, asList(event_1, event_2, event_3, event_4, event_5));

        successfullyArchiveUntilCaughtUp(liveEventSource);

        EventSource s3EventSource = createS3ArchivedEventSource();

        assertThat(s3EventSource.readAll().readLastEvent().get().eventRecord(),
                is(objectWith(EventRecord::streamId, stream_1)
                .and(EventRecord::eventNumber, 3L) // event is zero indexed
                .and(EventRecord::eventType, event_4.type())
                .and(EventRecord::data, event_4.data())
                .and(EventRecord::metadata, event_4.metadata())
                .and(EventRecord::timestamp, fixedEventTimestamp)));
    }

    @Test public void
    archiver_can_provide_max_position_stored_in_archive() throws Exception {
        EventSource liveEventSource = new InMemoryEventSource(new JavaInMemoryEventStore(fixedClock));

        liveEventSource.writeStream().write(stream_1, asList(event_1, event_2, event_3, event_4, event_5));

        S3Archiver archiverPriorToArchiving = createUnstartedArchiver(liveEventSource);

        assertThat(archiverPriorToArchiving.maxPositionInArchive(), equalTo(Optional.empty()));

        S3Archiver archiver = successfullyArchiveUntilCaughtUp(liveEventSource);

        assertThat(archiver.maxPositionInArchive(), equalTo(Optional.of(new S3ArchivePosition(4))));
    }

    @Test public void
    archiver_can_provide_max_position_stored_in_original_event_store() {
        EventSource liveEventSource = new InMemoryEventSource(new JavaInMemoryEventStore(fixedClock));

        S3Archiver archiver = createUnstartedArchiver(liveEventSource);

        assertThat(archiver.lastEventInLiveEventStore(), equalTo(Optional.empty()));
        liveEventSource.writeStream().write(stream_1, asList(event_1, event_2, event_3, event_4));
        assertThat(archiver.lastEventInLiveEventStore().map(ResolvedEvent::position), equalTo(Optional.of(liveEventSource.positionCodec().deserializePosition("4"))));
        liveEventSource.writeStream().write(stream_1, asList(event_5));
        assertThat(archiver.lastEventInLiveEventStore().map(ResolvedEvent::position), equalTo(Optional.of(liveEventSource.positionCodec().deserializePosition("5"))));
    }

    @Test public void
    archiver_stores_content_with_gzip_compression() throws Exception {
        EventSource liveEventSource = new InMemoryEventSource(new JavaInMemoryEventStore(fixedClock));

        liveEventSource.writeStream().write(stream_1, asList(event_1, event_2));

        successfullyArchiveUntilCaughtUp(liveEventSource);

        S3DownloadableStorageWithoutDestinationFile downloadableStorage = createDownloadableStorage();

        String onlyBatchKey = new S3ArchiveKeyFormat(eventStoreId).objectKeyFor(2L, "gz");
        assertThat(downloadableStorage.download(onlyBatchKey, (this::isGzipped)), is(true));

    }

    private boolean isGzipped(InputStream inputStream) {
        try {
            byte [] signature = new byte[2];
            int len = inputStream.read(signature);
            assertThat("Couldn't read signature (2 bytes) from input stream", len, equalTo(2));
            int head = ((int) signature[0] & 0xff) | ((signature[1] << 8) & 0xff00);
            IOUtils.drainInputStream(inputStream);
            return (GZIPInputStream.GZIP_MAGIC == head);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test public void
    archiver_resumes_subscription_from_last_archived_position() throws Exception {
        EventSource liveEventSource = new InMemoryEventSource(new JavaInMemoryEventStore(fixedClock));

        liveEventSource.writeStream().write(stream_1, asList(event_1, event_2, event_3, event_4, event_5));

        S3Archiver previousRunOfArchiver = successfullyArchiveUntilCaughtUp(liveEventSource);

        assertThat(previousRunOfArchiver.maxPositionInArchive(), equalTo(Optional.of(new S3ArchivePosition(4L))));

        EventSource spiedOnEventSource = Mockito.spy(liveEventSource);
        EventReader spiedOnEventReader = Mockito.spy(liveEventSource.readAll());
        doReturn(spiedOnEventReader).when(spiedOnEventSource).readAll();

        successfullyArchiveUntilCaughtUp(spiedOnEventSource);

        Mockito.verify(spiedOnEventReader).readAllForwards(JavaInMemoryEventStore.CODEC.deserializePosition("4"));
    }


    @Test public void
    archiver_provides_monitoring_components_from_live_event_store() {
        EventSource liveEventSource = new InMemoryEventSource(new JavaInMemoryEventStore(fixedClock));

        liveEventSource.writeStream().write(stream_1, asList(event_1, event_2, event_3, event_4, event_5));

        S3Archiver archiver = createUnstartedArchiver(liveEventSource);

        assertThat(archiver.monitoring(), allOf(
                hasItem(tuckerComponent(equalTo(Status.INFO), containsString("JavaInMemoryEventStore"))),
                hasItem(tuckerComponent(equalTo(Status.OK), containsString("Awaiting initial catchup. No events processed yet."))),
                hasItem(tuckerComponent(equalTo(Status.INFO), containsString("Awaiting initial catchup")))));
    }


    @Test public void
    max_position_from_archive_is_absent_when_there_is_no_events() {
        ListableStorage listableStorage = new S3ListableStorage(amazonS3, bucketName, 1);
        S3ArchiveMaxPositionFetcher fetcher = new S3ArchiveMaxPositionFetcher(listableStorage, eventStoreId);

        assertThat(fetcher.maxPosition(), equalTo(Optional.empty()));
    }

    @Test public void
    can_fetch_max_position_over_multiple_pages_of_objects() throws Exception {
        EventSource liveEventSource = new InMemoryEventSource(new JavaInMemoryEventStore(fixedClock));

        liveEventSource.writeStream().write(stream_1, asList(event_1, event_2, event_3, event_4, event_5));

        successfullyArchiveUntilCaughtUp(liveEventSource);

        S3ArchiveMaxPositionFetcher fetcher = new S3ArchiveMaxPositionFetcher(createListableStorage(), eventStoreId);

        assertThat(fetcher.maxPosition(), equalTo(Optional.of(4L)));
    }

    @SuppressWarnings("unchecked")
    @Test public void
    uploaded_objects_have_useful_metadata() throws Exception {
        EventSource liveEventSource = new InMemoryEventSource(new JavaInMemoryEventStore(fixedClock));

        liveEventSource.writeStream().write(stream_1, asList(event_1, event_2, event_3, event_4, event_5));

        System.setProperty("timgroup.app.version", "1.0.12345");
        successfullyArchiveUntilCaughtUp(liveEventSource);

        String lastestBatchKey = new S3ArchiveKeyFormat(eventStoreId).objectKeyFor(4L, "gz");

        S3Object s3Object = amazonS3.getObject(new GetObjectRequest(bucketName, lastestBatchKey));

        assertThat(s3Object.getObjectMetadata().getUserMetadata(), allOf(
                hasEntry(equalTo("hostname"), anything()),
                hasEntry("app_version", "1.0.12345"),
                hasEntry("app_name", testName),
                hasEntry("min_position", "3"),
                hasEntry("max_position", "4"),
                hasEntry("number_of_events_in_batch", "2"),
                hasEntry("max_event_timestamp", fixedEventTimestamp.toString()),
                hasEntry("max_event_stream_category", category_1),
                hasEntry("max_event_stream_id", stream_1.id()),
                hasEntry("max_event_event_type", event_4.type())
        ));
    }

    @SuppressWarnings("unchecked")
    @Test public void
    warns_when_archive_is_too_far_behind_live_event_store() throws Exception {
        EventSource liveEventSource = new InMemoryEventSource(new JavaInMemoryEventStore(fixedClock));
        S3Archiver archiver = createUnstartedArchiver(liveEventSource);

        assertThat(archiver.monitoring(), allOf(
                hasItem(tuckerComponent(equalTo(Status.OK), allOf(
                        containsString("up to date"),
                        containsString("max_position in live=[none]"),
                        containsString("max_position in archive=[none]")
                )))));

        liveEventSource.writeStream().write(stream_1, Collections.nCopies(3, event_1));
        assertThat(archiver.monitoring(), allOf(
                hasItem(tuckerComponent(equalTo(Status.OK), allOf(
                        containsString("up to date"),
                        containsString("max_position in live=3"),
                        containsString("max_position in archive=[none]")
                )))));


        S3Archiver caughtUpArchiver = successfullyArchiveUntilCaughtUp(liveEventSource);

        assertThat(caughtUpArchiver.monitoring(), allOf(
                hasItem(tuckerComponent(equalTo(Status.OK), allOf(
                        containsString("up to date"),
                        containsString("max_position in live=3"),
                        containsString("max_position in archive=2")
                )))));
        caughtUpArchiver.stop();

        liveEventSource.writeStream().write(stream_1, Collections.nCopies(5, event_1));

        S3Archiver restartedArchiver = createUnstartedArchiver(liveEventSource);

        assertThat(restartedArchiver.monitoring(), allOf(
                hasItem(tuckerComponent(equalTo(Status.OK), allOf(
                        containsString("up to date"),
                        containsString("max_position in live=8"),
                        containsString("max_position in archive=2")
            )))));

        liveEventSource.writeStream().write(stream_1, Collections.nCopies(1, event_1));

        assertThat(restartedArchiver.monitoring(), allOf(
                hasItem(tuckerComponent(equalTo(Status.WARNING), allOf(
                        containsString("Archive is stale compared to live event store"),
                        containsString("max_position in live=9"),
                        containsString("max_position in archive=2")
                )))));
    }

    private Matcher<Component> tuckerComponent(Matcher<Status> statusMatcher, Matcher<String> valueMatcher) {
        return allOf(
                new FeatureMatcher<Component, Status>(statusMatcher, "status", "status") {
                    @Override protected Status featureValueOf(Component actual) { return actual.getReport().getStatus(); }
                },
                new FeatureMatcher<Component, String>(valueMatcher, "value", "value") {
                    @Override protected String featureValueOf(Component actual) { return actual.getReport().getValue().toString(); }
                }
            );
    }

    // TODO:
    //  x gzip the content
    //  x resume archiving from last archived position
    //  x monitoring for bucket configuration (e.g. that it requires encryption)
    //  x monitoring that archiver is up to date enough (max position in archive > max position in event store - 2x batch size)
    //  x add metadata to S3 objects (similar to metadata we use in eventstores: application; version; as well as other potentially useful info like: batch size; min position (some of which may be redundant)
    //  x add metrics (max position in archive, s3 bucket size if possible, events awaiting upload)
    //  x expose useful information via Tucker (subscription components; last upload state; etc)
    // Optimisations:
    // - search for max position starts from max position of original event store and looks backward
    // - max position is cached in memory if reused anywhere (e.g. tucker components; metrics)

    private EventSource createS3ArchivedEventSource() throws IOException {
        return new S3ArchivedEventSource(createListableStorage(), createDownloadableStorage(), eventStoreId);
    }

    private S3DownloadableStorageWithoutDestinationFile createDownloadableStorage() throws IOException {
        return new S3DownloadableStorageWithoutDestinationFile(
                    new S3DownloadableStorage(amazonS3, Files.createTempDirectory(testName), bucketName),
                    amazonS3, bucketName);
    }

    private S3ListableStorage createListableStorage() {
        int maxKeysInListingToTriggerPagingBehaviour = 1;
        return new S3ListableStorage(amazonS3, bucketName, maxKeysInListingToTriggerPagingBehaviour);
    }

    private S3Archiver createUnstartedArchiver(EventSource liveEventSource) {
        S3ArchiveMaxPositionFetcher maxPositionFetcher = new S3ArchiveMaxPositionFetcher(createListableStorage(), eventStoreId);
        return S3Archiver.newS3Archiver(liveEventSource,
                createUploadableStorage(),
                eventStoreId,
                SubscriptionBuilder.eventSubscription("test"),
                twoEventsPerBatch,
                maxPositionFetcher,
                testName,
                metricRegistry,
                fixedClock,
                Collections.emptyList(),
                uncompressed_batch_size_limit_bytes);
    }

    private S3Archiver successfullyArchiveUntilCaughtUp(EventSource liveEventSource) throws InterruptedException {
        return successfullyArchiveUntilCaughtUp(fixedClock, liveEventSource);
    }

    private S3Archiver successfullyArchiveUntilCaughtUp(Clock clock, EventSource liveEventSource) throws InterruptedException {
        InitialCatchupFuture catchupFuture = new InitialCatchupFuture();
        SubscriptionBuilder subscription = SubscriptionBuilder.eventSubscription("test")
                .withRunFrequency(Duration.of(1, ChronoUnit.MILLIS))
                .publishingTo(catchupFuture);

        S3ListableStorage listableStorage = createListableStorage();
        S3Archiver archiver = S3Archiver.newS3Archiver(liveEventSource, createUploadableStorage(), eventStoreId, subscription,
                twoEventsPerBatch, new S3ArchiveMaxPositionFetcher(listableStorage, eventStoreId),
                testName, metricRegistry, clock, Collections.emptyList(), uncompressed_batch_size_limit_bytes);

        archiver.start();

        CompletableFuture.anyOf(catchupFuture, scheduleTimeout(Duration.ofSeconds(5L))).join();

        return archiver;
    }

    private S3UploadableStorageForInputStream createUploadableStorage() {
        return new S3UploadableStorageForInputStream(new S3UploadableStorage(amazonS3, bucketName), amazonS3, bucketName);
    }


    private static String randomCategory() {
        return "stream_" + UUID.randomUUID().toString().replace("-", "");
    }

    private static byte[] randomData() {
        return ("{\n  \"value\": \"" + UUID.randomUUID() + "\"\n}").getBytes(UTF_8);
    }

    private static CompletableFuture<Void> scheduleTimeout(Duration duration) {
        CompletableFuture<Void> completion = new CompletableFuture<>();
        Timer timer = new Timer(true);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                completion.completeExceptionally(new TimeoutException());
            }
        }, duration.toMillis());
        return completion;
    }
}
