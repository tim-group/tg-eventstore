package com.timgroup.filesystem.filefeedcache;

import com.timgroup.eventstore.archiver.S3ArchiveKeyFormat;
import com.timgroup.filefeed.reading.ReadableFeedStorage;

import java.util.Optional;

import static com.timgroup.filefeed.reading.StorageLocation.TimGroupEventStoreFeedStore;

public class FileFeedCacheMaxPositionFetcher {


    private final ReadableFeedStorage readableFeedStorage;
    private final S3ArchiveKeyFormat s3ArchiveKeyFormat;

    public FileFeedCacheMaxPositionFetcher(ReadableFeedStorage readableFeedStorage, S3ArchiveKeyFormat s3ArchiveKeyFormat) {
        this.readableFeedStorage = readableFeedStorage;
        this.s3ArchiveKeyFormat = s3ArchiveKeyFormat;
    }

    public Optional<Long> maxPosition() {
        return readableFeedStorage.list(TimGroupEventStoreFeedStore, s3ArchiveKeyFormat.eventStorePrefix())
                .stream()
                .reduce((r1, r2) -> r2)
                .map(s3ArchiveKeyFormat::positionValueFrom);
    }
}
