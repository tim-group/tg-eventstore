package com.timgroup.eventstore.archiver;

import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public final class DeadStoreBatchingPolicy implements BatchingPolicy {
    public static final String NAME = "DeadStoreBatchingPolicy";

    private final BatchingPolicy fixedSizeBatchingPolicy;
    private final Position lastEventPosition;
    private final AtomicReference<ResolvedEvent> lastEventInBatch = new AtomicReference<>();

    public DeadStoreBatchingPolicy(int batchSize, Position lastEventPosition) {
        this.fixedSizeBatchingPolicy = BatchingPolicy.fixedNumberOfEvents(batchSize);
        this.lastEventPosition = lastEventPosition;
    }

    @Override
    public void notifyAddedToBatch(ResolvedEvent event) {
        fixedSizeBatchingPolicy.notifyAddedToBatch(event);
        lastEventInBatch.set(event);
    }

    @Override
    public boolean ready() {
        return fixedSizeBatchingPolicy.ready() || lastEventInBatch.get().position().equals(lastEventPosition);
    }

    @Override
    public void reset() {
        fixedSizeBatchingPolicy.reset();
        lastEventInBatch.set(null);
    }

    @Override
    public boolean isStale(Optional<Long> maxPositionInArchive, Optional<Long> maxPositionInLive, Optional<EventRecord> lastEventInLive) {
        return false;
    }
}
