package com.timgroup.filesystem.filefeedcache;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;

import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public final class ArchiveToLiveEventReader implements EventReader {
    private final EventReader archive;
    private final EventReader live;
    private final Position maxArchivePosition;

    public ArchiveToLiveEventReader(EventSource archive, EventSource live, Position maxArchivePosition) {
        this(archive.readAll(), live.readAll(), maxArchivePosition);
    }

    public ArchiveToLiveEventReader(EventReader archive, EventReader live, Position maxArchivePosition) {
        this.archive = requireNonNull(archive);
        this.live = requireNonNull(live);
        this.maxArchivePosition = maxArchivePosition;
    }

    @Nonnull
    @Override
    public ArchiveToLivePosition emptyStorePosition() {
        return new ArchiveToLivePosition(archive.emptyStorePosition());
    }

    @Nonnull
    @Override
    public PositionCodec storePositionCodec() {
        return ArchiveToLivePosition.codec(archive.storePositionCodec(), live.storePositionCodec(), maxArchivePosition);
    }

    @Nonnull
    @CheckReturnValue
    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        ArchiveToLivePosition archiveToLivePositionExclusive = (ArchiveToLivePosition) positionExclusive;
        return ArchiveToLiveEventsForwardsSpliterator.transitionedStreamFrom( // TODO consider the case where positionExclusive is already after lastPositionInArchive
                archive.readAllForwards(archiveToLivePositionExclusive.underlying),
                live.readAllForwards(maxArchivePosition),
                archiveToLivePositionExclusive);
    }

    @Override
    public String toString() {
        return "ArchiveToLiveEventReader{" +
                "archive=" + archive +
                ", live=" + live +
                ", maxArchivePosition=" + maxArchivePosition +
                '}';
    }
}
