package com.timgroup.eventstore.archiver;

import com.timgroup.remotefilestorage.api.ListableStorage;

import java.util.Optional;

public final class S3ArchiveMaxPositionFetcher {

    private final ListableStorage listableStorage;
    private final String eventStoreId;
    private final S3ArchiveKeyFormat s3ArchiveKeyFormat;

    public S3ArchiveMaxPositionFetcher(ListableStorage listableStorage, String eventStoreId) {
        this.listableStorage = listableStorage;
        this.eventStoreId = eventStoreId;
        this.s3ArchiveKeyFormat = new S3ArchiveKeyFormat(eventStoreId);
    }

    public Optional<Long> maxPosition() {
        return listableStorage.list(eventStoreId, null)
                .reduce((r1, r2) -> r2)
                .map(s3Object -> s3Object.name)
                .map(s3ArchiveKeyFormat::positionValueFrom);
    }
}
