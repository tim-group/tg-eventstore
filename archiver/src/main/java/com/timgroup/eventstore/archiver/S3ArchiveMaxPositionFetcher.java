package com.timgroup.eventstore.archiver;

import com.timgroup.remotefilestorage.api.ListableStorage;

import java.util.Optional;

public final class S3ArchiveMaxPositionFetcher {

    private final ListableStorage listableStorage;
    private final S3ArchiveKeyFormat s3ArchiveKeyFormat;

    public S3ArchiveMaxPositionFetcher(ListableStorage listableStorage, S3ArchiveKeyFormat s3ArchiveKeyFormat) {
        this.listableStorage = listableStorage;
        this.s3ArchiveKeyFormat = s3ArchiveKeyFormat;
    }

    public Optional<Long> maxPosition() {
        return listableStorage.list(s3ArchiveKeyFormat.eventStorePrefix(), null)
                .reduce((r1, r2) -> r2)
                .map(s3Object -> s3Object.name)
                .map(s3ArchiveKeyFormat::positionValueFrom);
    }
}
