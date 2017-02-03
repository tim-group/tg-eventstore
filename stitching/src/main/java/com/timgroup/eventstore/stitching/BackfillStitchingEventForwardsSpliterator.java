package com.timgroup.eventstore.stitching;

import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.lang.Long.MAX_VALUE;

final class BackfillStitchingEventForwardsSpliterator implements Spliterator<ResolvedEvent> {

    private final Spliterator<ResolvedEvent> backfillSpliterator;
    private final Spliterator<ResolvedEvent> liveSpliterator;

    private StitchedPosition lastPosition;
    private boolean beforeStitch = true;

    private BackfillStitchingEventForwardsSpliterator(Stream<ResolvedEvent> backfillStream, Stream<ResolvedEvent> liveStream, StitchedPosition startingPositionExclusive) {
        this.backfillSpliterator = backfillStream.spliterator();
        this.liveSpliterator = liveStream.spliterator();
        this.lastPosition = startingPositionExclusive;
    }

    static Stream<ResolvedEvent> stitchedStreamFrom(Stream<ResolvedEvent> backfillStream, Stream<ResolvedEvent> liveStream, StitchedPosition startingPositionExclusive) {
        return StreamSupport.stream(new BackfillStitchingEventForwardsSpliterator(backfillStream, liveStream, startingPositionExclusive), false)
                .onClose(() -> {backfillStream.close(); liveStream.close();});
    }

    @Override
    public boolean tryAdvance(Consumer<? super ResolvedEvent> consumer) {
        boolean hasNext;
        if (beforeStitch) {
            hasNext = backfillSpliterator.tryAdvance(backfillConsumer(consumer));
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
            backfillSpliterator.forEachRemaining(backfillConsumer);
        }
        Consumer<? super ResolvedEvent> liveConsumer = liveConsumer(consumer);
        liveSpliterator.forEachRemaining(liveConsumer);
    }

    private Consumer<? super ResolvedEvent> backfillConsumer(Consumer<? super ResolvedEvent> consumer) {
        return re -> consumer.accept(new ResolvedEvent(new StitchedPosition(re.position(), lastPosition.livePosition), re.eventRecord()));
    }

    private Consumer<? super ResolvedEvent> liveConsumer(Consumer<? super ResolvedEvent> consumer) {
        return re -> consumer.accept(new ResolvedEvent(new StitchedPosition(lastPosition.backfillPosition, re.position()), re.eventRecord()));
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
