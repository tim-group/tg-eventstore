package com.timgroup.filesystem.filefeedcache;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.archiver.EventStoreArchiverProtos;
import com.timgroup.eventstore.archiver.ProtobufsEventIterator;
import com.timgroup.eventstore.archiver.S3ArchiveKeyFormat;
import com.timgroup.eventstore.archiver.S3ArchivePosition;
import com.timgroup.filefeed.reading.ReadableFeedStorage;
import com.timgroup.tucker.info.Component;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static com.timgroup.filefeed.reading.StorageLocation.TimGroupEventStoreFeedStore;
import static java.util.Objects.requireNonNull;

public final class FileFeedCacheEventSource implements EventReader, EventSource {
    private final String eventStoreId;
    private final ReadableFeedStorage downloadableStorage;
    private final S3ArchiveKeyFormat s3ArchiveKeyFormat; // TODO rename class and field to ArchiveKeyFormat? also, S3ArchivePosition to ArchivePosition?

    public FileFeedCacheEventSource(String eventStoreId, ReadableFeedStorage downloadableStorage, S3ArchiveKeyFormat s3ArchiveKeyFormat) {
        this.eventStoreId = eventStoreId;
        this.downloadableStorage = downloadableStorage;
        this.s3ArchiveKeyFormat = s3ArchiveKeyFormat;
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        S3ArchivePosition toReadFrom = (S3ArchivePosition) requireNonNull(positionExclusive);

        return listFeedFiles()
                .filter(fileName -> s3ArchiveKeyFormat.positionValueFrom(fileName) > toReadFrom.value)
                .flatMap(fileName -> loadEventMessages(fileName).stream())
                .filter(event -> event.getPosition() > toReadFrom.value)
                .map(FileFeedCacheEventSource::toResolvedEvent);
    }

    @Override
    public Optional<ResolvedEvent> readLastEvent() {
        return listFeedFiles()
                .reduce((olderFile, newerFile) -> newerFile)
                .flatMap(file -> lastElementOf(loadEventMessages(file)))
                .map(FileFeedCacheEventSource::toResolvedEvent);
    }

    @Override
    public Position emptyStorePosition() {
        return S3ArchivePosition.EMPTY_STORE_POSITION;
    }

    @Override
    public PositionCodec storePositionCodec() {
        return S3ArchivePosition.CODEC;
    }

    private Stream<String> listFeedFiles() {
        return downloadableStorage.list(TimGroupEventStoreFeedStore, s3ArchiveKeyFormat.eventStorePrefix()).stream();
    }

    private List<EventStoreArchiverProtos.Event> loadEventMessages(String fileName) {
        try (InputStream inputStream = downloadableStorage.get(TimGroupEventStoreFeedStore, fileName)) {
            return parseEventMessages(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<EventStoreArchiverProtos.Event> parseEventMessages(InputStream inputStream) throws IOException {
        List<EventStoreArchiverProtos.Event> events = new ArrayList<>();
        try (GZIPInputStream decompressor = new GZIPInputStream(inputStream)) {
            new ProtobufsEventIterator<>(EventStoreArchiverProtos.Event.parser(), decompressor).forEachRemaining(events::add);
        }
        return events;
    }

    private static ResolvedEvent toResolvedEvent(EventStoreArchiverProtos.Event event) {
        return new ResolvedEvent(
                new S3ArchivePosition(event.getPosition()),
                EventRecord.eventRecord(
                        Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos()),
                        StreamId.streamId(event.getStreamCategory(), event.getStreamId()),
                        event.getEventNumber(),
                        event.getEventType(),
                        event.getData().toByteArray(),
                        event.getMetadata().toByteArray()
                ));
    }

    private static <T> Optional<T> lastElementOf(List<? extends T> list) {
        return list.isEmpty() ? Optional.empty() : Optional.of(list.get(list.size() - 1));
    }

    @Nonnull @Override
    public EventReader readAll() {
        return this;
    }

    @Nonnull @Override
    public EventCategoryReader readCategory() {
        throw new UnsupportedOperationException();
    }

    @Nonnull @Override
    public EventStreamReader readStream() {
        throw new UnsupportedOperationException();
    }

    @Nonnull @Override
    public EventStreamWriter writeStream() {
        throw new UnsupportedOperationException();
    }

    @Nonnull @Override
    public Collection<Component> monitoring() {
        return Arrays.asList(new FileFeedCacheConnectionComponent(eventStoreId, new FileFeedCacheMaxPositionFetcher(downloadableStorage, s3ArchiveKeyFormat)));
    }
}
