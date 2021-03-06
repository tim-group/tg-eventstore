package com.timgroup.eventstore.mysql;

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
import com.timgroup.filefeed.reading.ReadableFeedStorage;
import com.timgroup.tucker.info.Component;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static com.timgroup.filefeed.reading.StorageLocation.TimGroupEventStoreFeedStore;
import static java.util.Objects.requireNonNull;

public final class FileFeedCacheEventSource implements EventSource, EventReader, EventCategoryReader {
    private final String eventStoreId;
    private final ReadableFeedStorage downloadableStorage;
    private final ArchiveKeyFormat archiveKeyFormat;
    private final Position maxArchivePosition;

    public FileFeedCacheEventSource(String eventStoreId, ReadableFeedStorage downloadableStorage, Position maxArchivePosition) {
        this.eventStoreId = eventStoreId;
        this.downloadableStorage = downloadableStorage;
        this.archiveKeyFormat = new ArchiveKeyFormat(eventStoreId);
        this.maxArchivePosition = maxArchivePosition;
    }

    @Nonnull @Override public EventReader readAll() { return this; }
    @Nonnull @Override public EventCategoryReader readCategory() { return this; }

    @Nonnull @Override public EventStreamReader readStream() { throw new UnsupportedOperationException(); }
    @Nonnull @Override public EventStreamWriter writeStream() { throw new UnsupportedOperationException(); }

    @Nonnull @Override public Collection<Component> monitoring() {
        return Collections.singletonList(
                new FileFeedCacheConnectionComponent(eventStoreId, new FileFeedCacheMaxPositionFetcher(downloadableStorage, archiveKeyFormat))
        );
    }

    @Override public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        BasicMysqlEventStorePosition mysqlPositionExclusive = (BasicMysqlEventStorePosition) requireNonNull(positionExclusive);
        return listFeedFiles()
                .filter(fileName -> archiveKeyFormat.positionValueFrom(fileName).value > mysqlPositionExclusive.value)
                .flatMap(fileName -> loadEventMessages(fileName).stream())
                .filter(event -> event.getPosition() > mysqlPositionExclusive.value)
                .map(FileFeedCacheEventSource::toResolvedEvent);
    }

    @Override public Optional<ResolvedEvent> readLastEvent() {
        return listFeedFiles()
                .reduce((olderFile, newerFile) -> newerFile)
                .flatMap(file -> lastElementOf(loadEventMessages(file)))
                .map(FileFeedCacheEventSource::toResolvedEvent);
    }

    @Override public Position emptyStorePosition() {
        return BasicMysqlEventStorePosition.EMPTY_STORE_POSITION;
    }
    @Override public PositionCodec storePositionCodec() {
        return BasicMysqlEventStorePosition.CODEC;
    }

    @Nonnull @Override
    public Stream<ResolvedEvent> readCategoryForwards(String category, Position positionExclusive) {
        return readAllForwards(positionExclusive).filter(re -> category.equals(re.eventRecord().streamId().category()));
    }

    @Nonnull @Override
    public Stream<ResolvedEvent> readCategoriesForwards(List<String> categories, Position positionExclusive) {
        return readAllForwards(positionExclusive).filter(re -> categories.contains(re.eventRecord().streamId().category()));
    }

    @Nonnull @Override public Position emptyCategoryPosition(String category) { return BasicMysqlEventStorePosition.EMPTY_STORE_POSITION; }
    @Nonnull @Override public PositionCodec categoryPositionCodec(String category) { return BasicMysqlEventStorePosition.CODEC; }


    private Stream<String> listFeedFiles() {
        return downloadableStorage
                .list(TimGroupEventStoreFeedStore, archiveKeyFormat.eventStorePrefix())
                .stream()
                .filter(fileName -> archiveKeyFormat.positionValueFrom(fileName).compareTo((BasicMysqlEventStorePosition) maxArchivePosition) <= 0);
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
                new BasicMysqlEventStorePosition(event.getPosition()),
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
}
