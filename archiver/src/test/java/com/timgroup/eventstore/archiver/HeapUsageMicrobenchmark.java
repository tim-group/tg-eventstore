package com.timgroup.eventstore.archiver;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.memory.InMemoryEventSource;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.time.Clock;
import java.util.UUID;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;

public class HeapUsageMicrobenchmark {

    public static void main(String[] args) throws IOException {
        Histogram compressedSizeMetrics = new Histogram(new ExponentiallyDecayingReservoir());
        Histogram uncompressedSizeMetrics = new Histogram(new ExponentiallyDecayingReservoir());;
        S3ArchiveKeyFormat batchS3ObjectKeyFormat = new S3ArchiveKeyFormat("AnyOldEventStoreId");

        int batchSize = 100_000;
        BatchingPolicy batchingPolicy = BatchingPolicy.fixedNumberOfEvents(batchSize);
        Clock clock = Clock.systemUTC();
        EventSource eventSource = new InMemoryEventSource(new JavaInMemoryEventStore(clock));
        Function<ResolvedEvent, Long> positionFrom = (event) -> Long.valueOf(eventSource.readAll().storePositionCodec().serializePosition(event.position()));
        StreamId streamId = StreamId.streamId(S3IntegrationTest.randomCategory(), "AnyStream");

//        HeapyCurrentBatchWriter currentBatchWriter = new HeapyCurrentBatchWriter(batchingPolicy, positionFrom, batchS3ObjectKeyFormat, uncompressedSizeMetrics, compressedSizeMetrics);
        CurrentBatchWriter currentBatchWriter = new CurrentBatchWriter(batchingPolicy, positionFrom, batchS3ObjectKeyFormat, uncompressedSizeMetrics, compressedSizeMetrics);


        for (int i = 0; i < batchSize; i++) {
            ResolvedEvent anyEvent = new ResolvedEvent(
                    eventSource.readAll().storePositionCodec().deserializePosition(String.valueOf(i)),
                    EventRecord.eventRecord(clock.instant(), streamId, i, "AnyEvent", randomData(), randomData()));

            currentBatchWriter.add(anyEvent);

            if (currentBatchWriter.readyToUpload()) {
                S3BatchObject s3BatchObject = currentBatchWriter.prepareBatchForUpload();
                System.out.println(s3BatchObject.metadata);
            }
        }

        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        System.out.println(humanReadableByteCount(memoryMXBean.getHeapMemoryUsage().getUsed()));
        System.out.println(memoryMXBean.getHeapMemoryUsage());
        System.out.println(memoryMXBean.getNonHeapMemoryUsage());
        System.out.println(memoryMXBean.getNonHeapMemoryUsage());
    }

    private static byte[] randomData() {
        return ("{\"value\": \"" + UUID.randomUUID() + "\",\"id\": \"1a8b8863-a859-4d68-b63a-c466e554fd13\",\n" +
                "  \"name\": \"Ada Lovelace\",\n" +
                "  \"email\": \"ada@geemail.com\",\n" +
                "  \"bio\": \"First programmer. No big deal.\",\n" +
                "  \"age\": 198,\n" +
                "  \"avatar\": \"http://en.wikipedia.org/wiki/File:Ada_lovelace.jpg\"}").getBytes(UTF_8);
    }

    static String humanReadableByteCount(long bytes) {
        boolean si = false;
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }
}
