package com.timgroup.filesystem.filefeedcache;

import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.lang.Long.MAX_VALUE;
import static java.util.Objects.requireNonNull;

public final class ArchiveToLiveEventsForwardsSpliterator implements Spliterator<ResolvedEvent> {
    private final Spliterator<ResolvedEvent> archiveSpliterator;
    private final Spliterator<ResolvedEvent> liveSpliterator;
    private ArchiveToLivePosition lastPosition;
    private boolean withinArchiveEvents = true;

    private ArchiveToLiveEventsForwardsSpliterator(Stream<ResolvedEvent> archiveStream, Stream<ResolvedEvent> liveStream, ArchiveToLivePosition startingPositionExclusive) {
        this.archiveSpliterator = archiveStream.spliterator();
        this.liveSpliterator = liveStream.spliterator();
        this.lastPosition = requireNonNull(startingPositionExclusive);
    }

    static Stream<ResolvedEvent> transitionedStreamFrom(Stream<ResolvedEvent> archiveStream, Stream<ResolvedEvent> liveStream, ArchiveToLivePosition startingPositionExclusive) {
        return StreamSupport.stream(new ArchiveToLiveEventsForwardsSpliterator(archiveStream, liveStream, startingPositionExclusive), false)
                .onClose(() -> {archiveStream.close(); liveStream.close();});
    }

    @Override
    public boolean tryAdvance(Consumer<? super ResolvedEvent> consumer) {
        boolean hasNext;
        if (withinArchiveEvents) {
            hasNext = archiveSpliterator.tryAdvance(backfillConsumer(consumer));
            if (!hasNext) {
                withinArchiveEvents = false;
                hasNext = liveSpliterator.tryAdvance(liveConsumer(consumer));
            }
        }
        else
            hasNext = liveSpliterator.tryAdvance(liveConsumer(consumer));
        return hasNext;
    }

    @Override
    public void forEachRemaining(Consumer<? super ResolvedEvent> consumer) {
        if (withinArchiveEvents) {
            Consumer<? super ResolvedEvent> backfillConsumer = backfillConsumer(consumer);
            archiveSpliterator.forEachRemaining(backfillConsumer);
        }
        Consumer<? super ResolvedEvent> liveConsumer = liveConsumer(consumer);
        liveSpliterator.forEachRemaining(liveConsumer);
    }

    private Consumer<? super ResolvedEvent> backfillConsumer(Consumer<? super ResolvedEvent> consumer) {
        return re -> {
            lastPosition = new ArchiveToLivePosition(re.position());
            consumer.accept(new ResolvedEvent(lastPosition, re.eventRecord()));
        };
    }

    private Consumer<? super ResolvedEvent> liveConsumer(Consumer<? super ResolvedEvent> consumer) {
        return re -> {
            lastPosition = new ArchiveToLivePosition(re.position());
            consumer.accept(new ResolvedEvent(lastPosition, re.eventRecord()));
        };
    }

    @Override
    public Spliterator<ResolvedEvent> trySplit() {
        return null;
    }

    @Override
    public long estimateSize() {
        return MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return ORDERED | NONNULL | DISTINCT;
    }
}
