package com.timgroup.eventstore.archiver;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.remotefilestorage.api.RemoteFileDetails;
import com.timgroup.remotefilestorage.s3.S3DownloadableStorageWithoutDestinationFile;
import com.timgroup.remotefilestorage.s3.S3ListableStorage;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;

import static java.util.stream.Collectors.toList;

public final class S3ArchivedEventReader implements EventReader {
    private final S3ListableStorage s3ListableStorage;
    private final S3DownloadableStorageWithoutDestinationFile s3DownloadableStorage;
    private final String eventStoreId;

    public S3ArchivedEventReader(
            S3ListableStorage s3ListableStorage,
            S3DownloadableStorageWithoutDestinationFile s3DownloadableStorage,
            String eventStoreId) {
        this.s3ListableStorage = s3ListableStorage;
        this.s3DownloadableStorage = s3DownloadableStorage;
        this.eventStoreId = eventStoreId;
    }

    @Nonnull
    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
       return s3ListableStorage.list(eventStoreId + "/", null).flatMap(this::getEventsFromMultiTry);
    }

    private Stream<ResolvedEvent> getEventsFromMultiTry(RemoteFileDetails remoteFileDetails) {
        int maxAttempts = 5;
        int attemptsSoFar = 0;
        Optional<Exception> lastException = Optional.empty();
        while(attemptsSoFar < maxAttempts) {
            attemptsSoFar += 1;
            try {
                return getEventsFrom(remoteFileDetails);
            } catch(Exception e) {
                lastException = Optional.of(e);
            }
        }
        throw new RuntimeException(String.format("Failed to download S3 file %s after %s attempts. Giving up! ", remoteFileDetails.name, attemptsSoFar), lastException.get());

    }

    private Stream<ResolvedEvent> getEventsFrom(RemoteFileDetails remoteFileDetails) {
        return s3DownloadableStorage.download(remoteFileDetails.name, this::deserialize);
    }

    private Stream<ResolvedEvent> deserialize(InputStream inputStream) {
        try (GZIPInputStream decompressor = new GZIPInputStream(inputStream)) {

            ProtobufsEventIterator<EventStoreArchiverProtos.Event> eventIterator = new ProtobufsEventIterator<>(EventStoreArchiverProtos.Event::parseFrom, decompressor);
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(eventIterator, Spliterator.IMMUTABLE | Spliterator.ORDERED | Spliterator.NONNULL), false)
                    .map(this::toResolvedEvent)
                    .collect(toList())
                    .stream();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private ResolvedEvent toResolvedEvent(EventStoreArchiverProtos.Event event) {
        Position position = new S3ArchivePosition(event.getPosition());
        Instant timestamp = Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos());
        StreamId streamId = StreamId.streamId(event.getStreamCategory(), event.getStreamId());
        return new ResolvedEvent(
                position,
                EventRecord.eventRecord(
                        timestamp,
                        streamId,
                        event.getEventNumber(),
                        event.getEventType(),
                        event.getData().toByteArray(),
                        event.getMetadata().toByteArray()
                ));
    }

    @Nonnull
    @Override
    public Optional<ResolvedEvent> readLastEvent() {
        return s3ListableStorage.list(eventStoreId + "/", null)
                .reduce((r1, r2) -> r2)
                .map(this::getEventsFrom)
                .flatMap(events -> events.reduce((e1, e2) -> e2));
    }

    @Nonnull
    @Override
    public Position emptyStorePosition() {
        return S3ArchivePosition.EMPTY_STORE_POSITION;
    }

    @Nonnull
    @Override
    public PositionCodec storePositionCodec() {
        return S3ArchivePosition.CODEC;
    }
}
