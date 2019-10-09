package com.timgroup.eventstore.archiver;

import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.Optional;

public final class FixedNumberOfEventsBatchingPolicy implements BatchingPolicy {
    private static final int BATCHES_BEHIND_TOLERANCE = 3;

    private final int configuredBatchSize;
    private int currentBatchSize = 0;

    public FixedNumberOfEventsBatchingPolicy(int configuredBatchSize) {
        this.configuredBatchSize = configuredBatchSize;
    }

    @Override
    public void notifyAddedToBatch(ResolvedEvent event) {
        currentBatchSize++;
    }

    @Override
    public boolean ready() {
        return (currentBatchSize > 0) && (currentBatchSize % configuredBatchSize == 0);
    }

    @Override
    public void reset() {
        currentBatchSize = 0;
    }

    @Override
    public boolean isStale(Optional<Long> maxPositionInArchive, Optional<Long> maxPositionInLive, Optional<EventRecord> lastEventInLive) {
        return enoughEventsInLiveToBeStale(maxPositionInLive) && archiveIsNotCloseEnoughToLive(maxPositionInLive.get(), maxPositionInArchive);
    }

    private boolean archiveIsNotCloseEnoughToLive(Long livePosition, Optional<Long> archivePosition) {
        return archivePosition.map(archivePos -> (livePosition - archivePos) > (BATCHES_BEHIND_TOLERANCE * configuredBatchSize)).orElse(true);
    }

    private Boolean enoughEventsInLiveToBeStale(Optional<Long> livePosition) {
        return livePosition.map(pos -> pos > configuredBatchSize * BATCHES_BEHIND_TOLERANCE).orElse(false);
    }
}
