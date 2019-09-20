package com.timgroup.eventstore.archiver;

import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.List;
import java.util.Optional;

public final class DeadStoreBatchingPolicy implements BatchingPolicy {
    public static final String NAME = "DeadStoreBatchingPolicy";

    private final BatchingPolicy fixedSizeBatchingPolicy;
    private Position lastEventPosition;

    public DeadStoreBatchingPolicy(int batchSize, Position lastEventPosition) {
        this.fixedSizeBatchingPolicy = BatchingPolicy.fixedNumberOfEvents(batchSize);
        this.lastEventPosition = lastEventPosition;
    }

    @Override
    public boolean ready(List<ResolvedEvent> batch) {
        return fixedSizeBatchingPolicy.ready(batch) || isAtMaxPosition(batch);
    }

    private boolean isAtMaxPosition(List<ResolvedEvent> batch) {
        ResolvedEvent lastEventInBatch = batch.get(batch.size() - 1);
        return lastEventInBatch.position().equals(lastEventPosition);
    }

    @Override
    public boolean isStale(Optional<Long> maxPositionInArchive, Optional<Long> maxPositionInLive, Optional<EventRecord> lastEventInLive) {
        return false;
    }
}
