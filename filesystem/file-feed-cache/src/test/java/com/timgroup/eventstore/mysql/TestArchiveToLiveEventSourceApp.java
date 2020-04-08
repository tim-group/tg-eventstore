package com.timgroup.eventstore.mysql;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.archiver.S3ArchiveKeyFormat;
import com.timgroup.filefeed.reading.HttpFeedCacheStorage;
import com.timgroup.filefeed.reading.ReadableFeedStorage;
import com.timgroup.filefeed.reading.StorageLocation;
import org.joda.time.Instant;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.timgroup.eventstore.mysql.BasicMysqlEventStorePosition.CODEC;
import static com.timgroup.filefeed.reading.StorageLocation.TimGroupEventStoreFeedStore;
import static java.util.stream.Collectors.toList;

public class TestArchiveToLiveEventSourceApp {
    private static final Properties CONFIG = new Properties();

    public static void main(String[] args) throws IOException {
        CONFIG.load(Files.newInputStream(Paths.get(System.getProperty("user.home") + "/config.properties"), StandardOpenOption.READ));

        // 37202767 tradeideasmonitor-Event events takes: 185 seconds to read
        // 78502289 marketdata-event events takes: 186 seconds to read
        // 30843350 instrumenteventdb-event events takes: 116 seconds to read
        readFromCacheAndLive("tradeideasmonitor-Event", "db.tradeideasmonitor.", "TIM_EVENT_STORE");
        readFromCacheAndLive("marketdata-event", "db.marketdata.", "event");
        readFromCacheAndLive("instrumenteventdb-event", "db.instrumenteventdb.", "event");
        printEventsAroundCutoverPeriod("tradeideasmonitor-Event", "db.tradeideasmonitor.", "TIM_EVENT_STORE");
        printEventsAroundCutoverPeriod("marketdata-event", "db.marketdata.", "event");
        printEventsAroundCutoverPeriod("instrumenteventdb-event", "db.instrumenteventdb.", "event");
        printEventsFromLocalDirectory(
                System.getProperty("user.home") + "/Downloads/s3-archive",
                "instrumenteventdb-event",
                "30750000", 10);

        compareS3AndLive("instrumenteventdb-event", "db.instrumenteventdb.", "event",
                System.getProperty("user.home") + "/Downloads/s3-archive",
                "30750952", 6);

        EventSource dbEventSource = BasicMysqlEventSource.pooledReadOnlyDbEventSource(CONFIG, "db.instrumenteventdb.", "event", "instrumenteventdb-event", new MetricRegistry());
        dbEventSource.readAll()
                .readAllForwards(dbEventSource.readAll().storePositionCodec().deserializePosition("30750952"))
                .limit(6)
                .forEach(System.out::println);
    }

    private static void printEventsAroundCutoverPeriod(String eventStoreId, String dbConfigPrefix, String dbTableName) {
        HttpFeedCacheStorage downloadableStorage = new HttpFeedCacheStorage(URI.create("http://latest-file-feed-cacheapp-vip.oy.net.local:8000"));
        ArchiveKeyFormat archiveKeyFormat = new ArchiveKeyFormat(eventStoreId);

        BasicMysqlEventStorePosition lastPositionInArchive = new FileFeedCacheMaxPositionFetcher(downloadableStorage, archiveKeyFormat).maxPosition().get();
        FileFeedCacheEventSource archive = new FileFeedCacheEventSource(eventStoreId, downloadableStorage, lastPositionInArchive);
        EventSource live = BasicMysqlEventSource.pooledReadOnlyDbEventSource(CONFIG, dbConfigPrefix, dbTableName, eventStoreId, new MetricRegistry());

        ArchiveToLiveEventSource archiveToLiveEventSource = new ArchiveToLiveEventSource(archive, live, lastPositionInArchive);

        System.out.println("*****" + eventStoreId);
        Position startingPositionExclusive = archive.storePositionCodec().deserializePosition(Long.toString(lastPositionInArchive.value - 5));
        archiveToLiveEventSource.readAllForwards(startingPositionExclusive).limit(10).forEach(System.out::println);
    }

    private static void readFromCacheAndLive(String eventStoreId, String dbConfigPrefix, String dbTableName) {
        HttpFeedCacheStorage downloadableStorage = new HttpFeedCacheStorage(URI.create("http://latest-file-feed-cacheapp-vip.oy.net.local:8000"));
        ArchiveKeyFormat archiveKeyFormat = new ArchiveKeyFormat(eventStoreId);

        BasicMysqlEventStorePosition lastPositionInArchive = new FileFeedCacheMaxPositionFetcher(downloadableStorage, archiveKeyFormat).maxPosition()
                .orElseThrow(() -> new RuntimeException("Can't determine the max position of feed: " + archiveKeyFormat.eventStorePrefix()));
        FileFeedCacheEventSource archive = new FileFeedCacheEventSource(eventStoreId, downloadableStorage, lastPositionInArchive);
        EventSource live = BasicMysqlEventSource.pooledReadOnlyDbEventSource(CONFIG, dbConfigPrefix, dbTableName, eventStoreId, new MetricRegistry());

        ArchiveToLiveEventSource archiveToLiveEventSource = new ArchiveToLiveEventSource(archive, live, lastPositionInArchive);

        AtomicReference<Position> finalPosition = new AtomicReference<>(archiveToLiveEventSource.emptyStorePosition());
        AtomicLong eventCount = new AtomicLong(0);
        long start = System.currentTimeMillis();

        archiveToLiveEventSource.readAllForwards().forEach(e -> {
            finalPosition.set(e.position());
            if (eventCount.incrementAndGet() % 1000000 == 0) {
                Duration inMillis = Duration.of((System.currentTimeMillis() - start), ChronoUnit.MILLIS);
                System.out.println(String.format("read %s to position %s in %s seconds (in archive: %s)",
                        eventStoreId, eventCount, inMillis.getSeconds(), ((BasicMysqlEventStorePosition)finalPosition.get()).compareTo(lastPositionInArchive) <= 0
                ));
            }
        });
        Duration inMillis = Duration.ofMillis((System.currentTimeMillis() - start));
        System.out.println(String.format("%s %s events takes: %s seconds to read", eventCount.get(), eventStoreId, inMillis.getSeconds()));
        System.out.println("Final position is: " + finalPosition.get());
    }

    private static void printEventsFromLocalDirectory(String localS3ArchiveDirectory, String eventStoreId, String startPositionExclusive, long maxResults) {
        LocalDirectoryReadableFeedStorage localDirStorage = new LocalDirectoryReadableFeedStorage(Paths.get(localS3ArchiveDirectory));
        localDirStorage.list(null, eventStoreId).forEach(System.out::println);

        FileFeedCacheEventSource localDirEventSource = new FileFeedCacheEventSource(eventStoreId, localDirStorage, CODEC.deserializePosition(Long.toString(Long.MAX_VALUE)));
        localDirEventSource.readAll()
                .readAllForwards(localDirEventSource.storePositionCodec().deserializePosition(startPositionExclusive))
                .limit(maxResults);
    }

    private static void compareS3AndLive(String eventStoreId, String dbConfigPrefix, String dbTableName, String localS3ArchiveDirectory, String startPositionExclusive, long maxResults) throws IOException {
        LocalDirectoryReadableFeedStorage localDirStorage = new LocalDirectoryReadableFeedStorage(Paths.get(localS3ArchiveDirectory));
        localDirStorage.list(null, eventStoreId).forEach(System.out::println);
        S3ArchiveKeyFormat s3ArchiveKeyFormat = new S3ArchiveKeyFormat(eventStoreId);

        EventSource localS3EventSource = new FileFeedCacheEventSource(eventStoreId, localDirStorage, CODEC.deserializePosition(Long.toString(Long.MAX_VALUE)));
        EventSource dbEventSource = BasicMysqlEventSource.pooledReadOnlyDbEventSource(CONFIG, dbConfigPrefix, dbTableName, eventStoreId, new MetricRegistry());

        Iterator<ResolvedEvent> db = dbEventSource.readAll()
                .readAllForwards(dbEventSource.readAll().storePositionCodec().deserializePosition(startPositionExclusive))
                .limit(maxResults).iterator();

        Iterator<ResolvedEvent> s3 = localS3EventSource.readAll()
                .readAllForwards(localS3EventSource.readAll().storePositionCodec().deserializePosition(startPositionExclusive))
                .limit(maxResults).iterator();

        while (db.hasNext()) {
            ResolvedEvent dbEvent = db.next();
            ResolvedEvent s3Event = s3.next();
            if (dbEvent.eventRecord().equals(s3Event.eventRecord())) {
                System.out.println("okay   " + dbEvent.locator() + " " + dbEvent.eventRecord().timestamp());
            } else {
                System.out.println("ERROR  " + dbEvent + " vs " + s3Event);
            }
        }
    }

    static final class LocalDirectoryReadableFeedStorage implements ReadableFeedStorage {
        private final Path directory;

        LocalDirectoryReadableFeedStorage(Path directory) {
            this.directory = directory;
        }

        @Override public Optional<Instant> getArrivalTime(StorageLocation storageLocation, String fileName) {
            return Optional.of(Instant.EPOCH); // TODO
        }

        @Override public InputStream get(StorageLocation storageLocation, String fileName) {
            Preconditions.checkArgument(storageLocation == TimGroupEventStoreFeedStore);
            try {
                return new FileInputStream(directory.resolve(fileName).toFile());
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        @Override public List<String> list(StorageLocation storageLocation, String feedName) {
            try {
                return Files.list(directory.resolve(feedName))
                        .map(path -> directory.relativize(path).toString())
                        .collect(toList());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
