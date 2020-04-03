package com.timgroup.filesystem.filefeedcache;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.tucker.info.Component;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public final class ArchiveToLiveEventSource implements EventReader, EventSource {
    private final EventSource archive;
    private final EventSource live;
    private final Position maxArchivePosition;

    public ArchiveToLiveEventSource(EventSource archive, EventSource live, Position maxArchivePosition) {
        this.archive = requireNonNull(archive);
        this.live = requireNonNull(live);
        this.maxArchivePosition = maxArchivePosition;
    }

    @Nonnull @Override
    public ArchiveToLivePosition emptyStorePosition() {
        return new ArchiveToLivePosition(archive.readAll().emptyStorePosition());
    }

    @Nonnull @Override
    public PositionCodec storePositionCodec() {
        return ArchiveToLivePosition.codec(archive.readAll().storePositionCodec(), live.readAll().storePositionCodec(), maxArchivePosition);
    }

    @Nonnull @CheckReturnValue @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        ArchiveToLivePosition archiveToLivePositionExclusive = (ArchiveToLivePosition) positionExclusive;
        return archiveToLivePositionExclusive.isArchivePosition()
                ? ArchiveToLiveEventsForwardsSpliterator.transitionedStreamFrom(
                        archive.readAll().readAllForwards(archiveToLivePositionExclusive.underlying),
                        live.readAll().readAllForwards(maxArchivePosition),
                        archiveToLivePositionExclusive)
                : ArchiveToLiveEventsForwardsSpliterator.transitionedStreamFrom(
                        Stream.empty(),
                        live.readAll().readAllForwards(archiveToLivePositionExclusive.underlying),
                        archiveToLivePositionExclusive);
    }

    @Override
    public String toString() {
        return "ArchiveToLiveEventSource{" +
                "archive=" + archive +
                ", live=" + live +
                ", maxArchivePosition=" + maxArchivePosition +
                '}';
    }

    @Nonnull @Override
    public EventReader readAll() {
        return this;
    }

    @Nonnull @Override
    public Collection<Component> monitoring() {
        List<Component> result = new ArrayList<>();
        result.addAll(archive.monitoring());
        result.addAll(live.monitoring());
        return result;
    }

    @Nonnull @Override public EventCategoryReader readCategory() { throw new UnsupportedOperationException(); }
    @Nonnull @Override public EventStreamReader readStream() { throw new UnsupportedOperationException(); }
    @Nonnull @Override public EventStreamWriter writeStream() { throw new UnsupportedOperationException(); }
}
