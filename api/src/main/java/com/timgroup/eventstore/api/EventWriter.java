package com.timgroup.eventstore.api;

import java.util.Collection;

public interface EventWriter {
    void write(StreamId streamId, Collection<NewEvent> events);

    void write(StreamId streamId, Collection<NewEvent> events, int expectedVersion);
}
