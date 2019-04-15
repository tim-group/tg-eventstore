package com.timgroup.eventstore.archiver;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.remotefilestorage.s3.S3DownloadableStorageWithoutDestinationFile;
import com.timgroup.remotefilestorage.s3.S3ListableStorage;
import com.timgroup.tucker.info.Component;

import javax.annotation.Nonnull;
import java.util.Collection;

public class S3ArchivedEventSource implements EventSource {
    private final S3ListableStorage s3ListableStorage;
    private final S3DownloadableStorageWithoutDestinationFile s3DownloadableStorage;
    private final String eventStoreId;

    public S3ArchivedEventSource(S3ListableStorage s3ListableStorage, S3DownloadableStorageWithoutDestinationFile s3DownloadableStorage, String eventStoreId) {

        this.s3ListableStorage = s3ListableStorage;
        this.s3DownloadableStorage = s3DownloadableStorage;
        this.eventStoreId = eventStoreId;
    }

    @Nonnull
    @Override
    public EventReader readAll() {
        return new S3ArchivedEventReader(s3ListableStorage, s3DownloadableStorage, eventStoreId);
    }

    @Nonnull
    @Override
    public EventCategoryReader readCategory() {
        throw new UnsupportedOperationException("readCategory not supported. Only readAll is supported.");
    }

    @Nonnull
    @Override
    public EventStreamReader readStream() {
        throw new UnsupportedOperationException("readCategory not supported. Only readAll is supported.");
    }

    @Nonnull
    @Override
    public EventStreamWriter writeStream() {
        throw new UnsupportedOperationException("readCategory not supported. Only readAll is supported.");
    }

    @Nonnull
    @Override
    public PositionCodec positionCodec() {
        return S3ArchivePosition.CODEC;
    }

    @Nonnull
    @Override
    public Collection<Component> monitoring() {
        return null;
    }
}
