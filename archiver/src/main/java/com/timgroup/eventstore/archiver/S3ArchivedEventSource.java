package com.timgroup.eventstore.archiver;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.archiver.monitoring.S3ArchiveConnectionComponent;
import com.timgroup.remotefilestorage.api.ListableStorage;
import com.timgroup.remotefilestorage.api.StreamingDownloadableStorage;
import com.timgroup.tucker.info.Component;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;

import static java.util.Objects.requireNonNull;

public final class S3ArchivedEventSource implements EventSource {
    private final ListableStorage listableStorage;
    private final StreamingDownloadableStorage streamingDownloadableStorage;
    private final String bucketName;
    private final String eventStoreId;
    private final S3ArchiveKeyFormat s3ArchiveKeyFormat;

    public S3ArchivedEventSource(@Nonnull ListableStorage listableStorage,
                                 @Nonnull StreamingDownloadableStorage streamingDownloadableStorage,
                                 @Nonnull String bucketName,
                                 @Nonnull String eventStoreId)
    {
        this.listableStorage = requireNonNull(listableStorage, "listableStorage");
        this.streamingDownloadableStorage = requireNonNull(streamingDownloadableStorage, "streamingDownloadableStorage");
        this.bucketName = requireNonNull(bucketName, "bucketName");
        this.eventStoreId = requireNonNull(eventStoreId, "eventStoreId");
        this.s3ArchiveKeyFormat = new S3ArchiveKeyFormat(this.eventStoreId);
    }

    @Nonnull
    @Override
    public EventReader readAll() {
        return new S3ArchivedEventReader(listableStorage, streamingDownloadableStorage, s3ArchiveKeyFormat);
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
                        new S3ArchiveMaxPositionFetcher(listableStorage, s3ArchiveKeyFormat)));
    }
}
