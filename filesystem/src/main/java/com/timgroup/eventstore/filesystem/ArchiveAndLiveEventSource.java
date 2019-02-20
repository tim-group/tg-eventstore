package com.timgroup.eventstore.filesystem;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.tucker.info.Component;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Comparator.comparing;
import static java.util.Comparator.nullsFirst;
import static java.util.Objects.requireNonNull;

public final class ArchiveAndLiveEventSource implements EventSource, EventReader {
    private final ArchiveDirectoryEventSource archiveEventSource;
    private final EventSource liveEventSource;

    public ArchiveAndLiveEventSource(Path archiveDirectory, EventSource liveEventSource) {
        archiveEventSource = new ArchiveDirectoryEventSource(archiveDirectory);
        this.liveEventSource = requireNonNull(liveEventSource, "liveEventSource");
    }

    @Nonnull
    @Override
    public Position emptyStorePosition() {
        return ArchiveAndLivePosition.EMPTY;
    }

    @Nonnull
    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        ArchiveAndLivePosition ourPositionExclusive = (ArchiveAndLivePosition) positionExclusive;
        if (ourPositionExclusive.getLivePosition() != null) {
            return liveEventSource.readAll().readAllForwards(ourPositionExclusive.getLivePosition()).map(re -> re.eventRecord().toResolvedEvent(ourPositionExclusive.withLivePosition(re.position())));
        }

        ArchiveDirectoryPosition archiveDirectoryPosition = ourPositionExclusive.getArchiveDirectoryPosition();
        Stream<ResolvedEvent> archiveStream = archiveEventSource.readAll().readAllForwards(archiveDirectoryPosition);
        Spliterator<ResolvedEvent> archiveSpliterator = archiveStream.spliterator();
        Spliterator<ResolvedEvent> combinedSpliterator = new Spliterator<ResolvedEvent>() {
            private boolean archiveExhausted = false;
            private ArchiveDirectoryPosition lastArchivePosition;
            private Spliterator<ResolvedEvent> liveSpliterator;

            @Override
            public boolean tryAdvance(Consumer<? super ResolvedEvent> action) {
                if (!archiveExhausted) {
                    boolean producedArchiveEvent = archiveSpliterator.tryAdvance(re -> {
                        ArchiveDirectoryPosition archiveDirectoryPosition = (ArchiveDirectoryPosition) re.position();
                        lastArchivePosition = archiveDirectoryPosition;
                        action.accept(re.eventRecord().toResolvedEvent(new ArchiveAndLivePosition(archiveDirectoryPosition, null)));
                    });
                    if (producedArchiveEvent) {
                        return true;
                    }
                    archiveExhausted = true;
                }

                openLiveStream();

                return liveSpliterator.tryAdvance(re -> {
                    action.accept(re.eventRecord().toResolvedEvent(new ArchiveAndLivePosition(lastArchivePosition, re.position())));
                });
            }

            @Override
            public void forEachRemaining(Consumer<? super ResolvedEvent> action) {
                if (!archiveExhausted) {
                    archiveSpliterator.forEachRemaining(re -> {
                        ArchiveDirectoryPosition archiveDirectoryPosition = (ArchiveDirectoryPosition) re.position();
                        lastArchivePosition = archiveDirectoryPosition;
                        action.accept(re.eventRecord().toResolvedEvent(new ArchiveAndLivePosition(archiveDirectoryPosition, null)));
                    });
                    archiveExhausted = true;
                }

                openLiveStream();

                liveSpliterator.forEachRemaining(re -> {
                    action.accept(re.eventRecord().toResolvedEvent(new ArchiveAndLivePosition(lastArchivePosition, re.position())));
                });
            }

            private void openLiveStream() {
                if (liveSpliterator == null) {
                    String sourcePositionString = archiveEventSource.readSourcePosition(lastArchivePosition);
                    Position cutoverSourcePosition = liveEventSource.positionCodec().deserializePosition(sourcePositionString);
                    Stream<ResolvedEvent> liveStream = liveEventSource.readAll().readAllForwards(cutoverSourcePosition); // TODO store close action
                    liveSpliterator = liveStream.spliterator();
                }
            }

            @Override
            public Spliterator<ResolvedEvent> trySplit() {
                return null;
            }

            @Override
            public long estimateSize() {
                return Long.MAX_VALUE;
            }

            @Override
            public int characteristics() {
                return archiveSpliterator.characteristics();
            }
        };

        return StreamSupport.stream(combinedSpliterator, false).onClose(archiveStream::close);
    }

    @Nonnull
    @Override
    public EventReader readAll() {
        return this;
    }

    @Nonnull
    @Override
    public EventCategoryReader readCategory() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public EventStreamReader readStream() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public EventStreamWriter writeStream() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public PositionCodec positionCodec() {
        return PositionCodec.fromComparator(ArchiveAndLivePosition.class,
                str -> ArchiveAndLivePosition.deserialise(str, liveEventSource.positionCodec()),
                pos -> ArchiveAndLivePosition.serialise(pos, liveEventSource.positionCodec()),
                comparing(ArchiveAndLivePosition::getArchiveDirectoryPosition)
                        .thenComparing(nullsFirst(comparing(ArchiveAndLivePosition::getLivePosition, liveEventSource.positionCodec()::comparePositions))));
    }

    @Nonnull
    @Override
    public Collection<Component> monitoring() {
        List<Component> combined = new ArrayList<>();
        combined.addAll(archiveEventSource.monitoring());
        combined.addAll(liveEventSource.monitoring());
        return combined;
    }
}
