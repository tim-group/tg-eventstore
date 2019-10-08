package com.timgroup.eventstore.archiver;

import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventsubscription.EventHandler;

import java.util.List;
import java.util.Optional;

public interface BatchingPolicy {
    void notifyAddedToBatch(ResolvedEvent event);
    boolean ready();
    void reset();

    boolean isStale(Optional<Long> maxPositionInArchive, Optional<Long> maxPositionInLive, Optional<EventRecord> lastEventInLive);

    static BatchingPolicy fixedNumberOfEvents(int eventsPerBatch) {
        return new FixedNumberOfEventsBatchingPolicy(eventsPerBatch);
    }

    /**
     * BatchingPolicy that is only suitable for "dead" event stores, where the final event has been written.
     *
     * The policy will behave like BatchingPolicy#fixedNumberOfEvents, up until it reads to the end of the event store,
     * at which point it will write a batch with size = (maxPosition % batchSize).
     *
     */
    static BatchingPolicy deadStore(int eventsPerBatch, Position lastEventPosition) {
        return new DeadStoreBatchingPolicy(eventsPerBatch, lastEventPosition);
    }

}
