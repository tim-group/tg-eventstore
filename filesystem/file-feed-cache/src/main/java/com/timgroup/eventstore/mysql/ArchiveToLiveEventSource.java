package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.filefeed.reading.ReadableFeedStorage;
import com.timgroup.tucker.info.Component;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public final class ArchiveToLiveEventSource implements EventSource, EventReader, EventCategoryReader, AutoCloseable  {
    private final EventSource archive;
    private final EventSource live;
    private final Position maxArchivePosition;

    public ArchiveToLiveEventSource(String eventStoreId, ReadableFeedStorage readableFeedStorage, EventSource live) {
        this.archive = new FileFeedCacheEventSource(requireNonNull(eventStoreId), requireNonNull(readableFeedStorage));
        this.live = requireNonNull(live);
        this.maxArchivePosition = new FileFeedCacheMaxPositionFetcher(readableFeedStorage, new ArchiveKeyFormat(eventStoreId))
                .maxPosition()
                .orElse(BasicMysqlEventStorePosition.EMPTY_STORE_POSITION);
    }

    ArchiveToLiveEventSource(EventSource archive, EventSource live, Position maxArchivePosition) {
        this.archive = requireNonNull(archive);
        this.live = requireNonNull(live);
        this.maxArchivePosition = maxArchivePosition;
    }

    @Nonnull @Override public EventReader readAll() { return this; }
    @Nonnull @Override public EventCategoryReader readCategory() { return this; }

    @Nonnull @Override public EventStreamReader readStream() { throw new UnsupportedOperationException(); }
    @Nonnull @Override public EventStreamWriter writeStream() { throw new UnsupportedOperationException(); }

    @Nonnull @Override public Collection<Component> monitoring() {
        List<Component> result = new ArrayList<>();
        result.addAll(archive.monitoring());
        result.addAll(live.monitoring());
        return result;
    }

    @Nonnull @CheckReturnValue @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        return canReadFromArchive(positionExclusive)
                ? Stream.concat(archive.readAll().readAllForwards(positionExclusive), live.readAll().readAllForwards(maxArchivePosition))
                : live.readAll().readAllForwards(positionExclusive);
    }

    @Nonnull @Override
    public Stream<ResolvedEvent> readCategoryForwards(String category, Position positionExclusive) {
        return canReadFromArchive(positionExclusive)
                ? Stream.concat(
                        archive.readCategory().readCategoryForwards(category, positionExclusive),
                        live.readCategory().readCategoryForwards(category, maxArchivePosition))
                : live.readCategory().readCategoryForwards(category, positionExclusive);
    }

    @Nonnull @Override
    public Stream<ResolvedEvent> readCategoriesForwards(List<String> categories, Position positionExclusive) {
        return canReadFromArchive(positionExclusive)
                ? Stream.concat(
                        archive.readCategory().readCategoriesForwards(categories, positionExclusive),
                        live.readCategory().readCategoriesForwards(categories, maxArchivePosition))
                : live.readCategory().readCategoriesForwards(categories, positionExclusive);
    }

    @Nonnull @Override public Position emptyStorePosition() { return BasicMysqlEventStorePosition.EMPTY_STORE_POSITION; }
    @Nonnull @Override public PositionCodec storePositionCodec() { return BasicMysqlEventStorePosition.CODEC; }
    @Nonnull @Override public Position emptyCategoryPosition(String category) { return BasicMysqlEventStorePosition.EMPTY_STORE_POSITION; }
    @Nonnull @Override public PositionCodec categoryPositionCodec(String category) { return BasicMysqlEventStorePosition.CODEC; }

    @Override public String toString() {
        return "ArchiveToLiveEventSource{archive=" + archive + ", live=" + live + ", maxArchivePosition=" + maxArchivePosition + '}';
    }

    private boolean canReadFromArchive(Position positionExclusive) {
        BasicMysqlEventStorePosition startingPositionExclusive = (BasicMysqlEventStorePosition) positionExclusive;
        return startingPositionExclusive.compareTo((BasicMysqlEventStorePosition) maxArchivePosition) < 0;
    }

    @Override
    public void close() throws Exception {
        if (live instanceof AutoCloseable) {
            ((AutoCloseable) live).close();
        }
    }
}