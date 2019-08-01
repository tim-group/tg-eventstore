package com.timgroup.eventstore.clock;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.merging.NamedReaderWithCodec;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static java.util.Objects.requireNonNull;

public final class TimeReader implements EventReader {
    private final Instant start;
    private final Duration accuracy;
    private final Clock clock;

    public TimeReader(Instant start, Duration accuracy, Clock clock) {
        this.start = requireNonNull(start);
        this.accuracy = requireNonNull(accuracy);
        this.clock = requireNonNull(clock);
    }

    @Nonnull
    public static NamedReaderWithCodec timePassedEventStream(Instant start, Duration accuracy, Clock clock) {
        TimeReader timeReader = new TimeReader(start, accuracy, clock);
        return new NamedReaderWithCodec("Clock", timeReader, TimePosition.CODEC);
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        long now = clock.millis();
        long lastTime = ((TimePosition) positionExclusive).value;

        long numberOfEvents = (now - lastTime) / accuracy.toMillis();

        return LongStream.range(0, numberOfEvents).map(operand -> lastTime + ((operand + 1) * accuracy.toMillis())).mapToObj(time -> new ResolvedEvent(
                new TimePosition(time),
                eventRecord(
                        Instant.ofEpochMilli(time),
                        StreamId.streamId("time", ""),
                        -1,
                        "TimePassed",
                        new byte[0],
                        new byte[0]
                )));
    }

    @Nonnull
    @Override
    public Position emptyStorePosition() {
        return new TimePosition(start.toEpochMilli());
    }

    @Nonnull
    @Override
    public PositionCodec storePositionCodec() {
        return TimePosition.CODEC;
    }

    @Override
    public String toString() {
        return "TimeReader{" +
                "start=" + start +
                ", accuracy=" + accuracy +
                ", clock=" + clock +
                '}';
    }

    private static final class TimePosition implements Position, Comparable<TimePosition> {
        static final PositionCodec CODEC = PositionCodec.ofComparable(TimePosition.class,
                string -> new TimePosition(Long.parseLong(string)),
                position -> Long.toString(position.value)
        );

        private final long value;

        private TimePosition(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return Instant.ofEpochMilli(value).toString();
        }

        @Override
        public boolean equals(@Nullable Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TimePosition that = (TimePosition) o;

            return value == that.value;
        }

        @Override
        public int hashCode() {
            return (int) (value ^ (value >>> 32));
        }

        @Override
        public int compareTo(TimePosition o) {
            return Long.compare(value, o.value);
        }
    }
}
