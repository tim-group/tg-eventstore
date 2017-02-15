package com.timgroup.eventstore.merging;

import com.timgroup.eventstore.api.EventInStream;

/**
 * @deprecated uaw MergedEventSource instead
 */
@Deprecated
public final class EventInIdentifiedStream {

    public final int streamIndex;
    public final EventInStream event;

    public EventInIdentifiedStream(int streamIndex, EventInStream event) {
        this.streamIndex = streamIndex;
        this.event = event;
    }
}
