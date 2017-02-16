package com.timgroup.eventstore.mysql;

import org.junit.Test;

import static com.timgroup.eventstore.api.StreamId.streamId;
import static java.util.Collections.emptyList;

public class BasicMysqlEventStreamWriterTest {
    @Test public void
    does_not_interact_with_database_when_no_events_are_written() {
        BasicMysqlEventStreamWriter writer = new BasicMysqlEventStreamWriter(null, null);

        writer.write(streamId("", ""), emptyList());
        writer.write(streamId("", ""), emptyList(), 5);
    }
}