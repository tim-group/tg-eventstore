package com.timgroup.eventstore.mysql;

public final class ArchiveKeyFormat {
    private final String eventStoreId;

    public ArchiveKeyFormat(String eventStoreId) {
        this.eventStoreId = eventStoreId;
    }

    public BasicMysqlEventStorePosition positionValueFrom(String batchS3ObjectKey) {
        String batchName = batchS3ObjectKey.replaceAll(eventStorePrefix(), "");
        return new BasicMysqlEventStorePosition(Long.parseLong(batchName.split("\\.")[0]));
    }

    public String eventStorePrefix() {
        return eventStoreId + "/";
    }
}