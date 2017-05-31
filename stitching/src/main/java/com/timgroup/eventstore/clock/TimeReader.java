package com.timgroup.eventstore.clock;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.merging.NamedReaderWithCodec;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;

public class TimeReader implements EventReader, PositionCodec {
    private final Instant start;
    private final Duration accuracy;
    private final Clock clock;

    public TimeReader(Instant start, Duration accuracy, Clock clock) {
        this.start = start;
        this.accuracy = accuracy;
        this.clock = clock;
    }

    public static NamedReaderWithCodec timePassedEventStream(Instant start, Duration accuracy, Clock clock) {
        TimeReader timeReader = new TimeReader(start, accuracy, clock);
        return new NamedReaderWithCodec("Clock", timeReader, timeReader);
    }

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

    @Override
    public Position emptyStorePosition() {
        return new TimePosition(start.toEpochMilli());
    }

    @Override
    public Position deserializePosition(String string) {
        return new TimePosition(Long.parseLong(string));
    }

    @Override
    public String serializePosition(Position position) {
        return Long.toString(((TimePosition) position).value);
    }

    private static final class TimePosition implements Position, Comparable<TimePosition> {
        private final long value;

        private TimePosition(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return Instant.ofEpochMilli(value).toString();
        }

        @Override
        public boolean equals(Object o) {
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
