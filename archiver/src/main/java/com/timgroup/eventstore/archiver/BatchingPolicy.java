package com.timgroup.eventstore.archiver;

import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.List;
import java.util.Optional;

interface BatchingPolicy {
    boolean ready(List<ResolvedEvent> batch);
    boolean isStale(Optional<S3ArchivePosition> maxPositionInArchive, Optional<ResolvedEvent> lastEventInLiveEventStore, PositionCodec liveEventSourceCodec);

    static BatchingPolicy fixedNumberOfEvents(int eventsPerBatch) {
        return new FixedNumberOfEventsBatchingPolicy(eventsPerBatch);
    }

}
