package com.timgroup.eventstore.stitching;

import com.timgroup.eventstore.api.EventInStream;

public final class EventInIdentifiedStream {

    public final int streamIndex;
    public final EventInStream event;

    public EventInIdentifiedStream(int streamIndex, EventInStream event) {
        this.streamIndex = streamIndex;
        this.event = event;
    }
}
