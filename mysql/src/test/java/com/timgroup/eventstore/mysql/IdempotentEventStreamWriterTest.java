package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.*;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.timgroup.eventstore.api.EventStreamReader.EmptyStreamEventNumber;
import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static com.timgroup.eventstore.mysql.IdempotentEventStreamWriter.idempotent;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class IdempotentEventStreamWriterTest {
    @Rule public final ExpectedException thrown = ExpectedException.none();

    private final JavaInMemoryEventStore store = new JavaInMemoryEventStore(Clock.systemUTC());
    private final EventStreamWriter underlying = store;
    private final EventStreamReader reader = store;
    private final StreamId stream = streamId("stream", "1");

    @Test public void
    successful_if_writes_same_data_again() {
        underlying
                .write(stream, singletonList(newEvent("type", "data".getBytes(), "metadata".getBytes())), EmptyStreamEventNumber);
        idempotent(underlying, reader)
                .write(stream, singletonList(newEvent("type", "data".getBytes(), "metadata".getBytes())), EmptyStreamEventNumber);

        assertThat(reader.readStreamForwards(stream).count(), is(1L));
    }

    @Test public void
    successful_if_metadata_is_different() {
        underlying
                .write(stream, singletonList(newEvent("type", "data".getBytes(), "metadata".getBytes())), EmptyStreamEventNumber);
        idempotent(underlying, reader)
                .write(stream, singletonList(newEvent("type", "data".getBytes(), "different metadata".getBytes())), EmptyStreamEventNumber);

        assertThat(reader.readStreamForwards(stream).count(), is(1L));
    }

    @Test public void
    throws_WrongExpectedVersionException_when_stream_moves_past_expected_version_during_save() {
        // Probably wrong semantics -- should "retry" to see if idempotent writes succeed
        thrown.expect(WrongExpectedVersionException.class);

        underlying
                .write(stream, singletonList(newEvent("type", "data".getBytes(), "metadata".getBytes())), EmptyStreamEventNumber);

        idempotent(underlying, reader, writeOnceSoToFailOptimisticConcurrencyCheck())
                .write(stream, asList(newEvent("type", "data".getBytes(), "metadata".getBytes()),
                                      newEvent("type", "data2".getBytes(), "metadata".getBytes())), EmptyStreamEventNumber);
    }

    @Test public void
    throws_IdempotentWriteFailure_for_different_event_with_the_same_version() {
        thrown.expect(IdempotentWriteFailure.class);

        underlying
                .write(stream, singletonList(newEvent("type", "data".getBytes(), "metadata".getBytes())), EmptyStreamEventNumber);
        idempotent(underlying, reader)
                .write(stream, singletonList(newEvent("type", "different data".getBytes(), "metadata".getBytes())), EmptyStreamEventNumber);
    }

    @Test public void
    successful_if_write_starts_later_in_stream() {
        underlying
                .write(stream, asList(newEvent("type", "data".getBytes(), "metadata".getBytes()),
                        newEvent("type", "data2".getBytes(), "metadata".getBytes())), EmptyStreamEventNumber);
        idempotent(underlying, reader)
                .write(stream, singletonList(newEvent("type", "data2".getBytes(), "metadata".getBytes())), 0);

        assertThat(reader.readStreamForwards(stream).count(), is(2L));
    }

    @Test public void
    successful_if_write_starts_later_in_stream_and_adds_new_data() {
        underlying
                .write(stream, asList(newEvent("type", "data".getBytes(), "metadata".getBytes()),
                                      newEvent("type", "data2".getBytes(), "metadata".getBytes())), EmptyStreamEventNumber);
        idempotent(underlying, reader)
                .write(stream, asList(newEvent("type", "data2".getBytes(), "metadata".getBytes()),
                                      newEvent("type", "data3".getBytes(), "metadata".getBytes())),
                        0);

        assertThat(reader.readStreamForwards(stream).count(), is(3L));
    }

    @Test public void
    fails_if_the_second_write_overlaps_but_doesnt_match_the_first() {
        thrown.expect(IdempotentWriteFailure.class);

        underlying
                .write(stream, asList(newEvent("type", "data".getBytes(), "metadata".getBytes()),
                        newEvent("type", "data2".getBytes(), "metadata".getBytes()),
                        newEvent("type", "data3".getBytes(), "metadata".getBytes())
                        ), EmptyStreamEventNumber);
        idempotent(underlying, reader)
                .write(stream, asList(
                        newEvent("type", "data2".getBytes(), "metadata".getBytes()),
                        newEvent("type", "different data".getBytes(), "metadata".getBytes())),
                        0);
    }

    @Test public void
    allows_custom_matching_to_match_otherwise_not_matching() {
        underlying
                .write(stream, asList(newEvent("type", "data".getBytes(), "metadata".getBytes()),
                        newEvent("type", "data2".getBytes(), "metadata".getBytes()),
                        newEvent("type", "data3".getBytes(), "metadata".getBytes())
                        ), EmptyStreamEventNumber);
        idempotent(underlying, reader, (a, b) -> {
            return;
        })
                .write(stream, asList(
                        newEvent("type", "data2".getBytes(), "metadata".getBytes()),
                        newEvent("type", "different data".getBytes(), "metadata".getBytes())),
                        0);
    }
    @Test public void
    allows_custom_matching_to_cause_not_matching() {
        IdempotentWriteFailure e = new IdempotentWriteFailure("Because I said say!");
        thrown.expect(equalTo(e));
        underlying
                .write(stream, asList(newEvent("type", "data".getBytes(), "metadata".getBytes()),
                        newEvent("type", "data2".getBytes(), "metadata".getBytes()),
                        newEvent("type", "data3".getBytes(), "metadata".getBytes())
                        ), EmptyStreamEventNumber);
        idempotent(underlying, reader, (a, b) -> {
            throw e;
        })
                .write(stream, asList(
                        newEvent("type", "data2".getBytes(), "metadata".getBytes()),
                        newEvent("type", "different data".getBytes(), "metadata".getBytes())),
                        0);
    }

    private IdempotentEventStreamWriter.CompatibilityPredicate writeOnceSoToFailOptimisticConcurrencyCheck() {
        AtomicBoolean shouldWrite = new AtomicBoolean(true);
        return (a, b) -> {
            if (shouldWrite.getAndSet(false)) {
                store.write(stream, singletonList(newEvent("type", "data2".getBytes(), "metadata".getBytes())));
            }
        };
    }

}