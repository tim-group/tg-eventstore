package com.timgroup.eventstore.stitching;

import com.timgroup.clocks.testing.LatchableClock;
import com.timgroup.eventstore.api.Body;
import com.timgroup.eventstore.api.EventData;
import com.timgroup.eventstore.api.EventInStream;
import org.joda.time.DateTime;
import org.junit.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.emptyIterator;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

public final class EventStoreStitchingIteratorTest {

    private final LatchableClock clock = new LatchableClock(Clock.systemUTC());

    private final TestConsumer outputStream = new TestConsumer();

    @Test
    public void merges_head_of_event_streams_by_oldest_event_first() throws Exception {
        Iterator<EventInIdentifiedStream> stream1 = newArrayList(event(1, "2016-01-01T10:00:02Z", 123423L)).iterator();
        Iterator<EventInIdentifiedStream> stream2 = newArrayList(event(2, "2016-01-01T10:00:01Z", 215224L)).iterator();

        clock.latchTo(Instant.parse("2016-01-01T10:00:02Z"));
        new EventStoreStitchingIterator(clock, Duration.ofSeconds(0), of(stream1, stream2)).forEachRemaining(outputStream);

        assertThat(outputStream.eventVersions(), contains(215224L, 123423L));
    }

    @Test
    public void delays_events_by_a_given_duration() {
        Iterator<EventInIdentifiedStream> stream1 = newArrayList(event(1, "2016-01-01T10:00:02Z", 123423L)).iterator();
        Iterator<EventInIdentifiedStream> stream2 = newArrayList(event(2, "2016-01-01T10:00:01Z", 215224L)).iterator();

        clock.latchTo(Instant.parse("2016-01-01T10:00:02Z"));
        new EventStoreStitchingIterator(clock, Duration.ofSeconds(2), of(stream1, stream2)).forEachRemaining(outputStream);
        assertThat(outputStream.eventVersions(), empty());
    }

    @Test
    public void only_reads_from_streams_up_to_the_time_any_stream_runs_out_of_events() {
        Iterator<EventInIdentifiedStream> emptyStream = new SlowIterator<>(clock, Duration.ofSeconds(2), emptyIterator());
        List<EventInIdentifiedStream> stream2 = newArrayList(event(2, "2016-01-01T10:00:01Z", 2222L));

        clock.latchTo(Instant.parse("2016-01-01T10:00:00Z"));
        new EventStoreStitchingIterator(clock, Duration.ofSeconds(0), of(emptyStream, stream2.iterator())).forEachRemaining(outputStream);
        assertThat(outputStream.eventVersions(), empty());

        clock.latchTo(Instant.parse("2016-01-01T10:00:00Z"));
        new EventStoreStitchingIterator(clock, Duration.ofSeconds(0), of(stream2.iterator(), emptyStream)).forEachRemaining(outputStream);
        assertThat(outputStream.eventVersions(), empty());
    }

    @Test
    public void reads_historic_events_from_a_stream_even_if_another_stream_runs_out_of_events() {
        Iterator<EventInIdentifiedStream> emptyStream = new SlowIterator<>(clock, Duration.ofSeconds(2), emptyIterator());
        List<EventInIdentifiedStream> stream2 = newArrayList(event(2, "2016-01-01T10:00:00Z", 2222L));

        clock.latchTo(Instant.parse("2016-01-01T10:00:00Z"));
        new EventStoreStitchingIterator(clock, Duration.ofSeconds(0), of(emptyStream, stream2.iterator())).forEachRemaining(outputStream);
        assertThat(outputStream.eventVersions(), contains(2222L));

        outputStream.events.clear();

        clock.latchTo(Instant.parse("2016-01-01T10:00:00Z"));
        new EventStoreStitchingIterator(clock, Duration.ofSeconds(0), of(stream2.iterator(), emptyStream)).forEachRemaining(outputStream);
        assertThat(outputStream.eventVersions(), contains(2222L));
    }

    @Test
    public void only_reads_from_streams_up_to_the_time_any_stream_runs_out_of_events_with_delay_applied() {
        Iterator<EventInIdentifiedStream> emptyStream = new SlowIterator<>(clock, Duration.ofSeconds(2), emptyIterator());
        List<EventInIdentifiedStream> stream2 = newArrayList(event(2, "2016-01-01T10:00:00Z", 2222L));

        clock.latchTo(Instant.parse("2016-01-01T10:00:00Z"));
        new EventStoreStitchingIterator(clock, Duration.ofSeconds(1), of(emptyStream, stream2.iterator())).forEachRemaining(outputStream);
        assertThat(outputStream.eventVersions(), empty());

        clock.latchTo(Instant.parse("2016-01-01T10:00:00Z"));
        new EventStoreStitchingIterator(clock, Duration.ofSeconds(1), of(stream2.iterator(), emptyStream)).forEachRemaining(outputStream);
        assertThat(outputStream.eventVersions(), empty());
    }

    @Test
    public void sets_the_cut_off_time_from_the_first_empty_stream_and_doesnt_adjust_it_thereafter() {
        Iterator<EventInIdentifiedStream> emptyStream1 = new SlowIterator<>(clock, Duration.ofSeconds(2), emptyIterator());
        Iterator<EventInIdentifiedStream> emptyStream2 = new SlowIterator<>(clock, Duration.ofSeconds(2), emptyIterator());
        List<EventInIdentifiedStream> stream2 = newArrayList(event(2, "2016-01-01T10:00:01Z", 2222L));

        clock.latchTo(Instant.parse("2016-01-01T10:00:00Z"));
        new EventStoreStitchingIterator(clock, Duration.ofSeconds(0), of(emptyStream1, emptyStream2, stream2.iterator())).forEachRemaining(outputStream);
        assertThat(outputStream.eventVersions(), empty());
    }

    private static EventInIdentifiedStream event(int streamIndex, String timestamp, long version) {
        final EventData eventData = new EventData("foo", Body.apply("".getBytes()));
        return new EventInIdentifiedStream(streamIndex, new EventInStream(DateTime.parse(timestamp), eventData, version));
    }

    private static final class TestConsumer implements Consumer<EventInIdentifiedStream> {
        public final List<EventInIdentifiedStream> events = new ArrayList<>();

        @Override
        public void accept(EventInIdentifiedStream eventInIdentifiedStream) {
            events.add(eventInIdentifiedStream);
        }

        public List<Long> eventVersions() {
            return events.stream().map(evt -> evt.event.version()).collect(Collectors.toList());
        }
    }

    private static class SlowIterator<E> implements Iterator<E> {
        private final LatchableClock clock;
        private final Iterator<E> underlying;
        private final Duration delay;

        public SlowIterator(LatchableClock clock, Duration delay, Iterator<E> underlying) {
            this.clock = clock;
            this.underlying = underlying;
            this.delay = delay;
        }

        @Override
        public boolean hasNext() {
            clock.bump(delay);
            return underlying.hasNext();
        }

        @Override
        public E next() {
            clock.bump(delay);
            return underlying.next();
        }
    }
}