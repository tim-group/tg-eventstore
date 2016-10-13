package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.api.WrongExpectedVersionException;

import java.time.Duration;
import java.util.Collection;

import static java.lang.Thread.sleep;
import static java.time.Duration.ofMillis;

public class RetryingEventStreamWriter implements EventStreamWriter {
    private final EventStreamWriter underlying;
    private final int count;
    private final Duration interval;

    private RetryingEventStreamWriter(EventStreamWriter underlying, int count, Duration interval) {
        this.underlying = underlying;
        this.count = count;
        this.interval = interval;
    }

    @Override
    public synchronized void write(StreamId streamId, Collection<NewEvent> events) {
        retry(() -> underlying.write(streamId, events));

    }

    @Override
    public synchronized void write(StreamId streamId, Collection<NewEvent> events, long expectedVersion) {
        retry(() -> underlying.write(streamId, events, expectedVersion));
    }

    private void retry(Runnable work) {
        int retriesRemaining = count;
        while (true) {
            try {
                work.run();
                return;
            } catch (WrongExpectedVersionException e) {
                throw e;
            } catch (RuntimeException e) {
                if (retriesRemaining-- == 0) {
                    throw e;
                }
                try {
                    sleep(interval.toMillis());
                } catch (InterruptedException e1) { }
            }
        }
    }

    public static EventStreamWriter retrying(EventStreamWriter underlying) {
        return new RetryingEventStreamWriter(underlying, 5, ofMillis(100));
    }

    public static EventStreamWriter retrying(EventStreamWriter underlying, int count, Duration interval) {
        return new RetryingEventStreamWriter(underlying, count, interval);
    }
}
