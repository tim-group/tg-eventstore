package com.timgroup.eventstore.archiver;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.archiver.monitoring.S3ArchiveConnectionComponent;
import com.timgroup.remotefilestorage.s3.S3ListableStorage;
import com.timgroup.remotefilestorage.s3.S3StreamingDownloadableStorage;
import com.timgroup.tucker.info.Component;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;

public final class S3ArchivedEventSource implements EventSource {
    private final S3ListableStorage s3ListableStorage;
    private final S3StreamingDownloadableStorage s3StreamingDownloadableStorage;
    private final String bucketName;
    private final String eventStoreId;
    private final S3ArchiveKeyFormat s3ArchiveKeyFormat;

    public S3ArchivedEventSource(S3ListableStorage s3ListableStorage,
                                 S3StreamingDownloadableStorage s3StreamingDownloadableStorage,
                                 String bucketName,
                                 String eventStoreId)
    {
        this.s3ListableStorage = s3ListableStorage;
        this.s3StreamingDownloadableStorage = s3StreamingDownloadableStorage;
        this.bucketName = bucketName;
        this.eventStoreId = eventStoreId;
        this.s3ArchiveKeyFormat = new S3ArchiveKeyFormat(eventStoreId);
    }

    @Nonnull
    @Override
    public EventReader readAll() {
        return new S3ArchivedEventReader(s3ListableStorage, s3StreamingDownloadableStorage, s3ArchiveKeyFormat);
    }

    @Nonnull
    @Override
    public EventCategoryReader readCategory() {
        throw new UnsupportedOperationException("readCategory not supported. Only readAll is supported.");
    }

    @Nonnull
    @Override
    public EventStreamReader readStream() {
        throw new UnsupportedOperationException("readStream not supported. Only readAll is supported.");
    }

    @Nonnull
    @Override
    public EventStreamWriter writeStream() {
        throw new UnsupportedOperationException("writeStream not supported. Only readAll is supported.");
    }

    @Nonnull
    @Override
    public Collection<Component> monitoring() {
        String id = "tg-eventstore-s3-archive-EventSource-connection-" + this.eventStoreId;
        String label = "S3 Archive EventStore (bucket=" + bucketName + ", eventStoreId=" + this.eventStoreId + ")";
        return Collections.singletonList(
                new S3ArchiveConnectionComponent(id, label, eventStoreId,
                        new S3ArchiveMaxPositionFetcher(s3ListableStorage, s3ArchiveKeyFormat)));
    }
}
