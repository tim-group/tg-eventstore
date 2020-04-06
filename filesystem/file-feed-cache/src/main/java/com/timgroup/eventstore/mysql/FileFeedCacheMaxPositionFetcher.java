package com.timgroup.eventstore.mysql;

import com.timgroup.filefeed.reading.ReadableFeedStorage;

import java.util.Optional;

import static com.timgroup.filefeed.reading.StorageLocation.TimGroupEventStoreFeedStore;

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
                .reduce((r1, r2) -> r2)
                .map(archiveKeyFormat::positionValueFrom);
    }
}
