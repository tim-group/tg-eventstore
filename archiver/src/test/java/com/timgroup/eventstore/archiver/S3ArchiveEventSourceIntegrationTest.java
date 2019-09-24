package com.timgroup.eventstore.archiver;

import com.amazonaws.services.s3.AmazonS3;
import com.timgroup.config.ConfigLoader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.archiver.monitoring.S3ArchiveConnectionComponent;
import com.timgroup.eventsubscription.Event;
import com.timgroup.eventsubscription.SubscriptionBuilder;
import com.timgroup.eventsubscription.healthcheck.InitialCatchupFuture;
import com.timgroup.remotefilestorage.s3.S3ClientFactory;
import com.timgroup.remotefilestorage.s3.S3DownloadableStorage;
import com.timgroup.remotefilestorage.s3.S3DownloadableStorageWithoutDestinationFile;
import com.timgroup.remotefilestorage.s3.S3ListableStorage;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.Report;
import com.timgroup.tucker.info.Status;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assume.assumeTrue;

public class S3ArchiveEventSourceIntegrationTest extends S3IntegrationTest {
    private AmazonS3 amazonS3;
    private String bucketName;
    private String eventStoreId;
    private String testClassName = getClass().getSimpleName();

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
        properties.setProperty("s3.accessKey", "AKAICOMPLETENONSENSE");
        amazonS3 = new S3ClientFactory().fromProperties(properties);
        EventSource s3ArchiveEventSource = createS3ArchivedEventSource();

        Component connectionComponent = getConnectionComponent(s3ArchiveEventSource);

        Report report = connectionComponent.getReport();

        assertThat(report.getStatus(), equalTo(Status.CRITICAL));
        assertThat(report.getValue().toString(), containsString("AmazonS3Exception"));
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

}
