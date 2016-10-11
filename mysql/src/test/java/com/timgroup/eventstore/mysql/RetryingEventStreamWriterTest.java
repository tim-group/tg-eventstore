package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.api.WrongExpectedVersion;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.Clock;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static com.timgroup.eventstore.mysql.RetryingEventStreamWriter.retrying;
import static java.time.Duration.ofMillis;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

public class RetryingEventStreamWriterTest {
    public final ExpectedException thrown = ExpectedException.none();
    private final JavaInMemoryEventStore underlying = new JavaInMemoryEventStore(Clock.systemUTC());
    private final StreamId stream = streamId("stream", "1");

    @Test public void
    successful_write_to_underlying() {
        retrying(underlying).write(stream, singletonList(newEvent("type", "data".getBytes(), "metadata".getBytes())));
        retrying(underlying).write(stream, singletonList(newEvent("type", "data".getBytes(), "metadata".getBytes())), 0);

        assertThat(underlying.readStreamForwards(stream).count(), is(2L));
    }

    @Test public void
    when_underlying_fails_but_eventually_succeeds_data_is_written() {
        retrying(failing(4, underlying), 5, ofMillis(1)).write(stream, singletonList(newEvent("type", "data".getBytes(), "metadata".getBytes())));
        retrying(failing(4, underlying), 5, ofMillis(1)).write(stream, singletonList(newEvent("type", "data".getBytes(), "metadata".getBytes())), 0);

        assertThat(underlying.readStreamForwards(stream).count(), is(2L));
    }

    @Test public void
    when_underlying_keeps_failing_it_propagates_failure_without_expected_version() {
        thrown.expect(RuntimeException.class);
        retrying(failing(5, underlying), 5, ofMillis(1)).write(stream, singletonList(newEvent("type", "data".getBytes(), "metadata".getBytes())));
    }

    @Test public void
    when_underlying_keeps_failing_it_propagates_failure_with_expected_version() {
        thrown.expect(RuntimeException.class);
        retrying(failing(5, underlying), 5, ofMillis(1)).write(stream, singletonList(newEvent("type", "data".getBytes(), "metadata".getBytes())), -1);
    }

    @Test public void
    when_wrong_expected_version_does_not_retry() {
        AtomicInteger writeCount = new AtomicInteger();

        try {
            retrying(new EventStreamWriter() {
                @Override
                public void write(StreamId streamId, Collection<NewEvent> events) {
                }

                @Override
                public void write(StreamId streamId, Collection<NewEvent> events, long expectedVersion) {
                    writeCount.incrementAndGet();
                    underlying.write(streamId, events, expectedVersion);
                }
            }).write(stream, singletonList(newEvent("type", "data".getBytes(), "metadata".getBytes())), 5);
            fail();
        } catch (WrongExpectedVersion e) {}

        assertThat(writeCount.get(), is(1));
    }

    private EventStreamWriter failing(int count, EventStreamWriter writer) {
        return new EventStreamWriter() {
            private int remaining = count;

            @Override
            public void write(StreamId streamId, Collection<NewEvent> events) {
                if (remaining == 0) {
                    writer.write(streamId, events);
                } else {
                    remaining--;
                    throw new RuntimeException("Failed");
                }
            }

            @Override
            public void write(StreamId streamId, Collection<NewEvent> events, long expectedVersion) {
                if (remaining == 0) {
                    writer.write(streamId, events, expectedVersion);
                } else {
                    remaining--;
                    throw new RuntimeException("Failed");
                }
            }
        };
    }
}