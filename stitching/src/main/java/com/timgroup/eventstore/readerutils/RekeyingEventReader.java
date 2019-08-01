package com.timgroup.eventstore.readerutils;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static java.lang.Long.parseLong;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.stream.StreamSupport.stream;

public final class RekeyingEventReader implements EventReader {

    private final EventReader underlying;
    private final PositionCodec underlyingPositionCodec;
    private final StreamId newStreamId;

    private RekeyingEventReader(EventReader underlying, PositionCodec underlyingPositionCodec, StreamId newStreamId) {
        this.underlying = requireNonNull(underlying);
        this.underlyingPositionCodec = requireNonNull(underlyingPositionCodec);
        this.newStreamId = requireNonNull(newStreamId);
    }

    public static RekeyingEventReader rekeying(EventReader underlying, PositionCodec underlyingPositionCodec, StreamId newKey) {
        return new RekeyingEventReader(underlying, underlyingPositionCodec, newKey);
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public final Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        RekeyedStreamPosition rekeyedEventPosition = (RekeyedStreamPosition)positionExclusive;

        Stream<ResolvedEvent> events = underlying.readAllForwards(rekeyedEventPosition.underlyingPosition);
        return stream(new RekeyingSpliterator(newStreamId, rekeyedEventPosition.eventNumber, events.spliterator()), false)
                .onClose(events::close);
    }

    @Nonnull
    @Override
    public final Position emptyStorePosition() {
        return new RekeyedStreamPosition(underlying.emptyStorePosition(), -1L);
    }

    @Nonnull
    public PositionCodec storePositionCodec() {
        return RekeyedStreamPosition.codec(underlyingPositionCodec);
    }


    @Override
    public String toString() {
        return "RekeyingEventReader{" +
                "underlying=" + underlying +
                ", underlyingPositionCodec=" + underlyingPositionCodec +
                ", newStreamId=" + newStreamId +
                '}';
    }

    private static final class RekeyingSpliterator implements Spliterator<ResolvedEvent> {
        private final StreamId newStreamId;
        private final Spliterator<ResolvedEvent> events;

        private long eventNumber;

        public RekeyingSpliterator(StreamId newStreamId, long lastEventNumber, Spliterator<ResolvedEvent> events) {
            this.newStreamId = newStreamId;
            this.eventNumber = lastEventNumber;
            this.events = events;
        }

        @Override
        public boolean tryAdvance(Consumer<? super ResolvedEvent> action) {
            return events.tryAdvance(rekey(action));
        }

        @Override
        public void forEachRemaining(Consumer<? super ResolvedEvent> action) {
            events.forEachRemaining(rekey(action));
        }

        @Override
        public Spliterator<ResolvedEvent> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return events.estimateSize();
        }

        @Override
        public int characteristics() {
            return ORDERED | NONNULL | DISTINCT | SIZED;
        }

        private Consumer<ResolvedEvent> rekey(Consumer<? super ResolvedEvent> action) {
            return event -> {
                eventNumber++;
                action.accept(new ResolvedEvent(
                        new RekeyedStreamPosition(event.position(), eventNumber),
                        eventRecord(
                                event.eventRecord().timestamp(),
                                newStreamId,
                                eventNumber,
                                event.eventRecord().eventType(),
                                event.eventRecord().data(),
                                event.eventRecord().metadata()
                        )
                ));
            };
        }
    }

    private static final class RekeyedStreamPosition implements Position {
        private static final String REKEY_SEPARATOR = ":";
        private static final Pattern REKEY_PATTERN = Pattern.compile(Pattern.quote(REKEY_SEPARATOR));

        private static PositionCodec codec(PositionCodec underlying) {
            return PositionCodec.fromComparator(RekeyedStreamPosition.class,
                    string -> {
                        String[] data = REKEY_PATTERN.split(string, 2);
                        return new RekeyedStreamPosition(
                                underlying.deserializePosition(data[1]),
                                parseLong(data[0])
                        );
                    },
                    position ->
                            position.eventNumber
                                + REKEY_SEPARATOR
                                + underlying.serializePosition(position.underlyingPosition),
                    comparing(pos -> pos.underlyingPosition, underlying::comparePositions));
        }

        private final Position underlyingPosition;
        private final long eventNumber;

        public RekeyedStreamPosition(Position position, long eventNumber) {
            this.underlyingPosition = position;
            this.eventNumber = eventNumber;
        }

        @Override
        public boolean equals(@Nullable Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RekeyedStreamPosition that = (RekeyedStreamPosition) o;
            return eventNumber == that.eventNumber &&
                    Objects.equals(underlyingPosition, that.underlyingPosition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(underlyingPosition, eventNumber);
        }

        @Override
        public String toString() {
            return "RekeyedStreamPosition{" +
                    "underlyingPosition=" + underlyingPosition +
                    ", eventNumber=" + eventNumber +
                    '}';
        }
    }
}
