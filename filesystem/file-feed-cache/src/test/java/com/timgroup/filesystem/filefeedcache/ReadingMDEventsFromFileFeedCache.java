package com.timgroup.filesystem.filefeedcache;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.archiver.S3ArchiveKeyFormat;
import com.timgroup.eventstore.merging.MergedEventReader;
import com.timgroup.eventstore.merging.MergingStrategy;
import com.timgroup.eventstore.merging.NamedReaderWithCodec;
import com.timgroup.filefeed.reading.HttpFeedCacheStorage;

import java.io.PrintWriter;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ReadingMDEventsFromFileFeedCache {
    private static AtomicLong eventCount = new AtomicLong(0);

    public static void main(String[] args) {
        HttpFeedCacheStorage downloadableStorage = new HttpFeedCacheStorage(URI.create("http://latest-file-feed-cacheapp-vip.oy.net.local:8000"));

        NamedReaderWithCodec marketdataEventReader = eventReaderFor(new FileFeedCacheEventReader(downloadableStorage, new S3ArchiveKeyFormat("marketdata-event")), "marketdata-event");
        FileFeedCacheEventReader imeEventReader = new FileFeedCacheEventReader(downloadableStorage, new S3ArchiveKeyFormat("tradeideasmonitor-Event"));
        NamedReaderWithCodec timIMEEventReader = eventReaderFor(imeEventReader, "tradeideasmonitor-Event");

        MergedEventReader eventReader = new MergedEventReader(Clock.systemUTC(), (MergingStrategy<Instant>) event ->
                event.eventRecord().timestamp(), marketdataEventReader, timIMEEventReader);

        long start = System.currentTimeMillis();

        try (PrintWriter printWriter = new PrintWriter("s3eventstoretimes.csv")) {
            printWriter.println("position,time(s)");
            eventReader.readAllForwards().forEach(e -> {
                Duration inMillis = Duration.of((System.currentTimeMillis() - start), ChronoUnit.MILLIS);
                long eventCount = ReadingMDEventsFromFileFeedCache.eventCount.incrementAndGet();
                if (eventCount % 100000 == 0) {
                    printWriter.println(String.format("%s,%s", eventCount, inMillis.getSeconds()));
                    System.out.println("read to position: " + eventCount + " " + inMillis.getSeconds());
                }
            });
            Duration inMillis = Duration.ofMillis((System.currentTimeMillis() - start));
            printWriter.println(String.format("%s,%s", eventCount.get(), inMillis.getSeconds()));
            System.out.println(String.format("%s marketdataevents events takes: %s seconds to read", eventCount.get(), inMillis.getSeconds()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static NamedReaderWithCodec eventReaderFor(EventReader eventReader, String eventStoreId) {
        return NamedReaderWithCodec.fromEventReader(eventStoreId, eventReader);
    }
}
