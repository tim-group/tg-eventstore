package com.timgroup.eventstore.archiver;

public final class S3ArchiveKeyFormat {

    private static final String ALPHANUMERIC_SORT_CONSISTENT_WITH_POSITION_SORT_FORMAT = "%032d";

    private final String eventStoreId;

    public S3ArchiveKeyFormat(String eventStoreId) {
        this.eventStoreId = eventStoreId;
    }

    public Long positionValueFrom(String batchS3ObjectKey) {
        String batchName = batchS3ObjectKey.replaceAll(eventStorePrefix(), "");
        return Long.parseLong(batchName.split("\\.")[0]);
    }

    public String objectKeyFor(Long maxPosition, String fileExtension) {
        return eventStorePrefix() + String.format(ALPHANUMERIC_SORT_CONSISTENT_WITH_POSITION_SORT_FORMAT, maxPosition) + "." + fileExtension;
    }

    public String eventStorePrefix() {
        return eventStoreId + "/";
    }
}
