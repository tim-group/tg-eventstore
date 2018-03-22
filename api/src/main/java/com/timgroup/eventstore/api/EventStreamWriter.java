package com.timgroup.eventstore.api;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Collection;

@ParametersAreNonnullByDefault
public interface EventStreamWriter {
    void write(StreamId streamId, Collection<NewEvent> events);

    void write(StreamId streamId, Collection<NewEvent> events, long expectedVersion);
}
