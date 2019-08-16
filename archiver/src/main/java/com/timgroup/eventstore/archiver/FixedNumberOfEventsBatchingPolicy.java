package com.timgroup.eventstore.archiver;

import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.List;
import java.util.Optional;

public final class FixedNumberOfEventsBatchingPolicy implements BatchingPolicy {
    private static final int BATCHES_BEHIND_TOLERANCE = 3;

    private final int batchSize;

    public FixedNumberOfEventsBatchingPolicy(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public boolean ready(List<ResolvedEvent> batch) {
        return !batch.isEmpty() && batch.size() % batchSize == 0;
    }

    @Override
    public boolean isStale(Optional<S3ArchivePosition> maxPositionInArchive, Optional<ResolvedEvent> lastEventInLiveEventStore, PositionCodec liveEventSourceCodec) {
        Optional<Long> archivePosition = maxPositionInArchive.map(pos -> pos.value);
        Optional<Long> livePosition = lastEventInLiveEventStore
                .map(ResolvedEvent::position)
                .map(liveEventSourceCodec::serializePosition)
                .map(Long::valueOf);
        //noinspection OptionalGetWithoutIsPresent
        return enoughEventsInLiveToBeStale(livePosition)
               && archiveIsNotCloseEnoughToLive(livePosition.get(), archivePosition)  ;
    }

    private boolean archiveIsNotCloseEnoughToLive(Long livePosition, Optional<Long> archivePosition) {
        return archivePosition.map(archivePos -> (livePosition - archivePos) > (BATCHES_BEHIND_TOLERANCE * batchSize)).orElse(true);
    }

    private Boolean enoughEventsInLiveToBeStale(Optional<Long> livePosition) {
        return livePosition.map(pos -> pos > batchSize * BATCHES_BEHIND_TOLERANCE).orElse(false);
    }
}
