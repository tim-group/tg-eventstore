package com.timgroup.filesystem.filefeedcache;

import com.codahale.metrics.MetricRegistry;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.archiver.S3ArchiveKeyFormat;
import com.timgroup.eventstore.merging.MergedEventReader;
import com.timgroup.eventstore.merging.MergingStrategy;
import com.timgroup.eventstore.merging.NamedReaderWithCodec;
import com.timgroup.eventstore.mysql.BasicMysqlEventSource;
import com.timgroup.eventstore.stitching.BackfillStitchingEventSource;
import com.timgroup.filefeed.reading.HttpFeedCacheStorage;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class ReadingMDEventsFromFileFeedCache {
    private static AtomicLong eventCount = new AtomicLong(0);

    public static void main(String[] args) throws IOException {
        if (Boolean.parseBoolean(args[1])) {
            readEventsFromCache();
        } else {
            readFromCacheAndLive();


        }
    }

    private static void readFromCacheAndLive() throws IOException {
        Properties config = new Properties();
        config.load(Files.newInputStream(Paths.get("/home/mshah/config.properties"), StandardOpenOption.READ));

        HttpFeedCacheStorage downloadableStorage = new HttpFeedCacheStorage(URI.create("http://latest-file-feed-cacheapp-vip.oy.net.local:8000"));
        S3ArchiveKeyFormat s3ArchiveKeyFormat = new S3ArchiveKeyFormat("marketdata-event");
        FileFeedCacheMaxPositionFetcher maxPositionFetcher = new FileFeedCacheMaxPositionFetcher(downloadableStorage, s3ArchiveKeyFormat);


        final FileFeedCacheEventSource fileFeedCacheEventSource = new FileFeedCacheEventSource(downloadableStorage, new S3ArchiveKeyFormat("marketdata-event"));
        MetricRegistry metricsRegistry = new MetricRegistry();

        EventSource marketDataEventSource = BasicMysqlEventSource.pooledReadOnlyDbEventSource(
                config,
                "db.marketdata.",
                "event",
                "marketdata",
                metricsRegistry
        );
        final Position position1 = maxPositionFetcher.maxPosition().map(position -> marketDataEventSource.readAll().storePositionCodec().deserializePosition(Long.toString(position))).get();

        final BackfillStitchingEventSource backfillStitchingEventSource = new BackfillStitchingEventSource(fileFeedCacheEventSource, marketDataEventSource, position1);

        long start = System.currentTimeMillis();
        try (PrintWriter printWriter = new PrintWriter("s3eventstoretimes.csv")) {
            printWriter.println("position,time(s)");
            backfillStitchingEventSource.readAll().readAllForwards().forEach(e -> {
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

    private static void readEventsFromCache() {
        HttpFeedCacheStorage downloadableStorage = new HttpFeedCacheStorage(URI.create("http://latest-file-feed-cacheapp-vip.oy.net.local:8000"));

        NamedReaderWithCodec marketdataEventReader = eventReaderFor(new FileFeedCacheEventSource(downloadableStorage, new S3ArchiveKeyFormat("marketdata-event")), "marketdata-event");
        FileFeedCacheEventSource imeEventReader = new FileFeedCacheEventSource(downloadableStorage, new S3ArchiveKeyFormat("tradeideasmonitor-Event"));
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
