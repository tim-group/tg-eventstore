package com.timgroup.eventstore.archiver;

import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class DeadStoreBatchingPolicyTest {

    private final DeadStoreBatchingPolicy deadStoreBatchingPolicy = new DeadStoreBatchingPolicy(2, new LongPosition(123));

    @Test public void
    batch_is_ready_when_it_reaches_the_configured_batch_size() {
        asList(new ResolvedEvent(new LongPosition(1), eventRecord()),
                new ResolvedEvent(new LongPosition(2), eventRecord()))
            .forEach(deadStoreBatchingPolicy::notifyAddedToBatch);

        assertThat(deadStoreBatchingPolicy.ready(), equalTo(true));
    }

    @Test public void
    batch_is_not_ready_if_it_is_smaller_than_the_configured_batch_size_and_has_not_reached_the_max_position() {
        asList(
                new ResolvedEvent(new LongPosition(122), eventRecord())
        ).forEach(deadStoreBatchingPolicy::notifyAddedToBatch);

        assertThat(deadStoreBatchingPolicy.ready(), equalTo(false));
    }

    @Test public void
    batch_is_ready_if_it_is_smaller_than_the_configured_batch_size_but_has_reached_the_max_position() {
        asList(
                new ResolvedEvent(new LongPosition(123), eventRecord())
        ).forEach(deadStoreBatchingPolicy::notifyAddedToBatch);

        assertThat(deadStoreBatchingPolicy.ready(), equalTo(true));
    }

    private EventRecord eventRecord() {
        return EventRecord.eventRecord(Instant.EPOCH, StreamId.streamId("any", "any"), 1234, "type", new byte[0], new byte[0]);
    }

    private static final class LongPosition implements Position {
        private long value;

        LongPosition(long value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LongPosition that = (LongPosition) o;
            return value == that.value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }
    }
}