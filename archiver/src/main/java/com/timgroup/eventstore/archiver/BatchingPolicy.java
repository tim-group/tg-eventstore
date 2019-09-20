package com.timgroup.eventstore.archiver;

import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.List;
import java.util.Optional;

public interface BatchingPolicy {
    boolean ready(List<ResolvedEvent> batch);
    boolean isStale(Optional<Long> maxPositionInArchive, Optional<Long> maxPositionInLive, Optional<EventRecord> lastEventInLive);

    static BatchingPolicy fixedNumberOfEvents(int eventsPerBatch) {
        return new FixedNumberOfEventsBatchingPolicy(eventsPerBatch);
    }

}
