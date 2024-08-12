package com.timgroup.eventstore.archiver;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.memory.InMemoryEventSource;
import com.timgroup.remotefilestorage.s3.UploadStorage;
import junit.framework.TestCase;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class BatchingUploadHandlerTest {
    private final InMemoryEventSource store = new InMemoryEventSource();
    private final TestStorage uploadableStorage = new TestStorage();
    private final BatchingUploadHandler handler = new BatchingUploadHandler(uploadableStorage,
            new CurrentBatchWriter(new FixedNumberOfEventsBatchingPolicy(1),
                    rs -> Long.parseLong(store.readAll().storePositionCodec().serializePosition(rs.position())),
                    new S3ArchiveKeyFormat("test"),
                    new MetricRegistry().histogram("test"),
                    new MetricRegistry().histogram("test")),
            Clock.systemUTC(),
            new HashMap<>(),
            "",
            new MetricRegistry().timer("timer"),
            1
    );

    @Test
    public void uploads_when_ready() {
        store.writeStream().write(StreamId.streamId("cat", "id"), Arrays.asList(
                NewEvent.newEvent("test", "hello".getBytes(StandardCharsets.UTF_8))
        ));
        ResolvedEvent evt = store.readAll().readAllForwards().findFirst().get();

        handler.apply(evt.position(), new S3Archiver.EventRecordHolder(evt.eventRecord()));

        assertThat(uploadableStorage.count, is(1));
    }

    @Test
    public void retries_upload_on_failure() {
        uploadableStorage.failNumberOfTimes(5);

        store.writeStream().write(StreamId.streamId("cat", "id"), Arrays.asList(
                NewEvent.newEvent("test", "hello".getBytes(StandardCharsets.UTF_8))
        ));
        ResolvedEvent evt = store.readAll().readAllForwards().findFirst().get();

        handler.apply(evt.position(), new S3Archiver.EventRecordHolder(evt.eventRecord()));

        assertThat(uploadableStorage.count, is(1));
    }

    public static class TestStorage implements UploadStorage {
        int count = 0;
        private int failCount = 0;

        @Override
        public URI upload(String name, InputStream content, long contentLength, Map<String, String> metaData) {
            if (failCount == 0) {
                count++;
                try {
                    return new URI("s3://nothing");
                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }
            }
            failCount--;
            throw new RuntimeException("Configured failure");
        }

        public void failNumberOfTimes(int failCount) {
            this.failCount = failCount;
        }
    }

}