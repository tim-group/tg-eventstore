package com.timgroup.eventstore.mysql;

import com.timgroup.filefeed.reading.ReadableFeedStorage;

import java.time.Instant;
import java.util.Comparator;
import java.util.Optional;

import static com.timgroup.filefeed.reading.StorageLocation.TimGroupEventStoreFeedStore;
import static org.joda.time.Instant.ofEpochMilli;

public class FileFeedCacheMaxPositionFetcher implements MaxPositionFetcher {


    private final ReadableFeedStorage readableFeedStorage;
    private final ArchiveKeyFormat archiveKeyFormat;

    public FileFeedCacheMaxPositionFetcher(ReadableFeedStorage readableFeedStorage, ArchiveKeyFormat archiveKeyFormat) {
        this.readableFeedStorage = readableFeedStorage;
        this.archiveKeyFormat = archiveKeyFormat;
    }

    public Optional<BasicMysqlEventStorePosition> maxPosition() {
        return readableFeedStorage.list(TimGroupEventStoreFeedStore, archiveKeyFormat.eventStorePrefix())
                .stream()
                .sorted(Comparator.reverseOrder())
                .map(archiveKeyFormat::positionValueFrom)
                .findFirst();
    }

    @Override
    public Optional<BasicMysqlEventStorePosition> maxPositionBefore(Instant cutOff) {
        return readableFeedStorage.list(TimGroupEventStoreFeedStore, archiveKeyFormat.eventStorePrefix())
                .stream()
                .sorted(Comparator.reverseOrder())
                .filter(file -> readableFeedStorage
                        .getArrivalTime(TimGroupEventStoreFeedStore, file)
                        .map(fileArrivalTime -> !fileArrivalTime.isAfter(ofEpochMilli(cutOff.toEpochMilli())))
                        .orElse(false))
                .map(archiveKeyFormat::positionValueFrom)
                .findFirst();
    }
}
