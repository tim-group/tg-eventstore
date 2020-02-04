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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

public final class S3ArchivedEventReader implements EventReader {
    private static <T> Optional<T> lastElementOf(List<? extends T> list) {
        if (list.isEmpty())
            return Optional.empty();
        else
            return Optional.of(list.get(list.size() - 1));
    }

    private final S3ListableStorage s3ListableStorage;
    private final S3DownloadableStorageWithoutDestinationFile s3DownloadableStorage;
    private final S3ArchiveKeyFormat s3ArchiveKeyFormat;

    public S3ArchivedEventReader(
            S3ListableStorage s3ListableStorage,
            S3DownloadableStorageWithoutDestinationFile s3DownloadableStorage,
            S3ArchiveKeyFormat s3ArchiveKeyFormat) {
        this.s3ListableStorage = s3ListableStorage;
        this.s3DownloadableStorage = s3DownloadableStorage;
        this.s3ArchiveKeyFormat = s3ArchiveKeyFormat;
    }

    @Nonnull
    @Override
    public Stream<ResolvedEvent> readAllForwards(@Nonnull Position positionExclusive) {
        S3ArchivePosition toReadFrom = (S3ArchivePosition) positionExclusive;

        return listAllBatches()
                .filter(batchesEndingWithPositionGreaterThan(toReadFrom))
                .flatMap(this::getEventsFromMultiTry)
                .filter(fromPosition(toReadFrom));
    }

    private Stream<RemoteFileDetails> listAllBatches() {
        return s3ListableStorage.list(s3ArchiveKeyFormat.eventStorePrefix(), null);
    }

    private Predicate<ResolvedEvent> fromPosition(S3ArchivePosition toReadFrom) {
        return (event) -> ((S3ArchivePosition)event.position()).value >= toReadFrom.value;
    }

    private Predicate<RemoteFileDetails> batchesEndingWithPositionGreaterThan(S3ArchivePosition toReadFrom) {
        return (batchFile) -> s3ArchiveKeyFormat.positionValueFrom(batchFile.name) >= toReadFrom.value;
    }

    private Stream<ResolvedEvent> getEventsFromMultiTry(RemoteFileDetails remoteFileDetails) {
        int maxAttempts = 5;
        int attemptsSoFar = 0;
        Optional<Exception> lastException = Optional.empty();
        while(attemptsSoFar < maxAttempts) {
            attemptsSoFar += 1;
            try {
                return loadEventMessages(remoteFileDetails).stream().map(this::toResolvedEvent);
            } catch(Exception e) {
                lastException = Optional.of(e);
            }
        }
        throw new RuntimeException(String.format("Failed to download S3 file %s after %s attempts. Giving up! ", remoteFileDetails.name, attemptsSoFar), lastException.get());

    }

    @Nonnull
    private List<EventStoreArchiverProtos.Event> loadEventMessages(RemoteFileDetails remoteFileDetails) {
        return s3DownloadableStorage.download(remoteFileDetails.name, this::parseEventMessages);
    }

    @Nonnull
    private List<EventStoreArchiverProtos.Event> parseEventMessages(InputStream inputStream) {
        List<EventStoreArchiverProtos.Event> events = new ArrayList<>();
        try (GZIPInputStream decompressor = new GZIPInputStream(inputStream)) {
            new ProtobufsEventIterator<>(EventStoreArchiverProtos.Event.parser(), decompressor).forEachRemaining(events::add);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return events;
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
        return listAllBatches()
                .reduce((r1, r2) -> r2)
                .flatMap(file -> lastElementOf(loadEventMessages(file)))
                .map(this::toResolvedEvent);
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
