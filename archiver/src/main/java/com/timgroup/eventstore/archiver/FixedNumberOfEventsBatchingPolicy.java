package com.timgroup.eventstore.archiver;

import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class FixedNumberOfEventsBatchingPolicy implements BatchingPolicy {
    private static final int BATCHES_BEHIND_TOLERANCE = 3;

    private final int batchSize;
    private final AtomicInteger currentBatchSize = new AtomicInteger(0);

    public FixedNumberOfEventsBatchingPolicy(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public void notifyAddedToBatch(ResolvedEvent event) {
        currentBatchSize.incrementAndGet();
    }

    @Override
    public boolean ready() {
        return (currentBatchSize.get() > 0) && (currentBatchSize.get() % batchSize == 0);
    }

    @Override
    public void reset() {
        currentBatchSize.set(0);
    }

    @Override
    public boolean isStale(Optional<Long> maxPositionInArchive, Optional<Long> maxPositionInLive, Optional<EventRecord> lastEventInLive) {
        return enoughEventsInLiveToBeStale(maxPositionInLive) && archiveIsNotCloseEnoughToLive(maxPositionInLive.get(), maxPositionInArchive);
    }

    private boolean archiveIsNotCloseEnoughToLive(Long livePosition, Optional<Long> archivePosition) {
        return archivePosition.map(archivePos -> (livePosition - archivePos) > (BATCHES_BEHIND_TOLERANCE * batchSize)).orElse(true);
    }

    private Boolean enoughEventsInLiveToBeStale(Optional<Long> livePosition) {
        return livePosition.map(pos -> pos > batchSize * BATCHES_BEHIND_TOLERANCE).orElse(false);
    }
}
