package com.timgroup.filesystem.filefeedcache;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.archiver.EventStoreArchiverProtos;
import com.timgroup.eventstore.archiver.ProtobufsEventIterator;
import com.timgroup.eventstore.archiver.S3ArchiveKeyFormat;
import com.timgroup.eventstore.archiver.S3ArchivePosition;
import com.timgroup.filefeed.reading.HttpFeedCacheStorage;
import com.timgroup.filefeed.reading.StorageLocation;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static java.util.Objects.requireNonNull;

public final class FileFeedCacheEventReader implements EventReader {

    private final HttpFeedCacheStorage downloadableStorage;
    private final S3ArchiveKeyFormat s3ArchiveKeyFormat;

    public FileFeedCacheEventReader(HttpFeedCacheStorage downloadableStorage, S3ArchiveKeyFormat s3ArchiveKeyFormat) {
        this.downloadableStorage = downloadableStorage;
        this.s3ArchiveKeyFormat = s3ArchiveKeyFormat;
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        S3ArchivePosition toReadFrom = (S3ArchivePosition) requireNonNull(positionExclusive);

        return listFeedFiles()
                .filter(batchesEndingWithPositionGreaterThan(toReadFrom))
                .flatMap(this::getEventsFromMultiTry)
                .filter(fromPosition(toReadFrom))
                .map(this::toResolvedEvent);
    }

    private Stream<String> listFeedFiles() {
        return downloadableStorage.list(StorageLocation.TimGroupEventStoreFeedStore, s3ArchiveKeyFormat.eventStorePrefix()).stream();
    }

    private Predicate<EventStoreArchiverProtos.Event> fromPosition( S3ArchivePosition toReadFrom) {
        return (event) -> event.getPosition() >= toReadFrom.value;
    }

    private Predicate<String> batchesEndingWithPositionGreaterThan( S3ArchivePosition toReadFrom) {
        return (fileName) -> s3ArchiveKeyFormat.positionValueFrom(fileName) >= toReadFrom.value;
    }

    private Stream<EventStoreArchiverProtos.Event> getEventsFromMultiTry(String fileName) {
        int maxAttempts = 5;
        int attemptsSoFar = 0;
        Optional<Exception> lastException = Optional.empty();
        while(attemptsSoFar < maxAttempts) {
            attemptsSoFar += 1;
            try {
                return loadEventMessages(fileName).stream();
            } catch(Exception e) {
                lastException = Optional.of(e);
            }
        }
        throw new RuntimeException(String.format("Failed to download S3 file %s after %s attempts. Giving up! ", fileName, attemptsSoFar), lastException.get());

    }


    private List<EventStoreArchiverProtos.Event> loadEventMessages(String fileName) {
        StorageLocation timGroupEventStoreFeedStore = StorageLocation.TimGroupEventStoreFeedStore;
        Optional<org.joda.time.Instant> arrivalTime = downloadableStorage.getArrivalTime(timGroupEventStoreFeedStore, fileName);
        if (arrivalTime.isPresent()) {
            try (InputStream inputStream = downloadableStorage.get(timGroupEventStoreFeedStore, fileName)) {
                return parseEventMessages(inputStream);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return Collections.emptyList();
    }


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


    @Override
    public Optional<ResolvedEvent> readLastEvent() {
        return listFeedFiles()
                .reduce((r1, r2) -> r2)
                .flatMap(file -> lastElementOf(loadEventMessages(file)))
                .map(this::toResolvedEvent);
    }

    private static <T> Optional<T> lastElementOf(List<? extends T> list) {
        if (list.isEmpty())
            return Optional.empty();
        else
            return Optional.of(list.get(list.size() - 1));
    }


    @Override
    public Position emptyStorePosition() {
        return S3ArchivePosition.EMPTY_STORE_POSITION;
    }


    public PositionCodec storePositionCodec() {
        return S3ArchivePosition.CODEC;
    }


}
