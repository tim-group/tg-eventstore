package com.timgroup.filesystem.filefeedcache;

import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.lang.Long.MAX_VALUE;
import static java.util.Objects.requireNonNull;

public class TransitioningEventsForwardsSpliterator implements Spliterator<ResolvedEvent> {


    private final Spliterator<ResolvedEvent> transitionSpliterator;
    private final Spliterator<ResolvedEvent> liveSpliterator;
    private TransitionPosition lastPosition;
    private boolean beforeStitch = true;

    private TransitioningEventsForwardsSpliterator(Stream<ResolvedEvent> archiveStream, Stream<ResolvedEvent> liveStream, TransitionPosition startingPositionExclusive) {
        this.transitionSpliterator = archiveStream.spliterator();
        this.liveSpliterator = liveStream.spliterator();
        this.lastPosition = requireNonNull(startingPositionExclusive);
    }

    static Stream<ResolvedEvent> transitionedStreamFrom(Stream<ResolvedEvent> archiveStream, Stream<ResolvedEvent> liveStream, TransitionPosition startingPositionExclusive) {
        return StreamSupport.stream(new TransitioningEventsForwardsSpliterator(archiveStream, liveStream, startingPositionExclusive), false)
                .onClose(() -> {archiveStream.close(); liveStream.close();});
    }

    @Override
    public boolean tryAdvance(Consumer<? super ResolvedEvent> consumer) {
        boolean hasNext;
        if (beforeStitch) {
            hasNext = transitionSpliterator.tryAdvance(backfillConsumer(consumer));
            if (!hasNext) {
                beforeStitch = false;
                hasNext = liveSpliterator.tryAdvance(liveConsumer(consumer));
            }
        }
        else
            hasNext = liveSpliterator.tryAdvance(liveConsumer(consumer));
        return hasNext;
    }

    @Override
    public void forEachRemaining(Consumer<? super ResolvedEvent> consumer) {
        if (beforeStitch) {
            Consumer<? super ResolvedEvent> backfillConsumer = backfillConsumer(consumer);
            transitionSpliterator.forEachRemaining(backfillConsumer);
        }
        Consumer<? super ResolvedEvent> liveConsumer = liveConsumer(consumer);
        liveSpliterator.forEachRemaining(liveConsumer);
    }

    private Consumer<? super ResolvedEvent> backfillConsumer(Consumer<? super ResolvedEvent> consumer) {
        return re -> {
            lastPosition = new TransitionPosition(re.position());
            consumer.accept(new ResolvedEvent(lastPosition, re.eventRecord()));
        };
    }

    private Consumer<? super ResolvedEvent> liveConsumer(Consumer<? super ResolvedEvent> consumer) {
        return re -> {
            lastPosition = new TransitionPosition(re.position());
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
