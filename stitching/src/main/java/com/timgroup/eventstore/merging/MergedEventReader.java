package com.timgroup.eventstore.merging;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;

import java.time.Instant;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static java.lang.Long.MAX_VALUE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

public final class MergedEventReader<T extends Comparable<T>> implements EventReader {

    private final MergingStrategy<T> mergingStrategy;
    private final EventReader[] readers;

    @SuppressWarnings("WeakerAccess")
    public MergedEventReader(MergingStrategy<T> mergingStrategy, EventReader... readers) {
        this.mergingStrategy = mergingStrategy;
        this.readers = readers;
    }

    public static MergedEventReader<Instant> effectiveTimestampMergedEventReader(EventReader... readers) {
        return new MergedEventReader<>(new EffectiveTimestampMergingStrategy(), readers);
    }

    public static MergedEventReader<Integer> streamOrderMergedEventReader(EventReader... readers) {
        return new MergedEventReader<>(new StreamIndexMergingStrategy(), readers);
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        return StreamSupport.stream(new MergingSpliterator<>(mergingStrategy, (MergedEventReaderPosition) positionExclusive, readers), false);
    }

    private static final class MergingSpliterator<T extends Comparable<T>> implements Spliterator<ResolvedEvent> {

        private final MergingStrategy<T> mergingStrategy;
        private final EventReader[] readers;

        private MergedEventReaderPosition currentPosition;

        private MergingSpliterator(MergingStrategy<T> mergingStrategy, MergedEventReaderPosition startPosition, EventReader... readers) {
            this.mergingStrategy = mergingStrategy;
            this.readers = readers;
            this.currentPosition = startPosition;
        }

        @Override
        public boolean tryAdvance(Consumer<? super ResolvedEvent> consumer) {
            Optional<ResolvedEvent> maybeNextInputEvent = advanceToNextEvent();
            if (maybeNextInputEvent.isPresent()) {
                EventRecord record = maybeNextInputEvent.get().eventRecord();
                consumer.accept(new ResolvedEvent(
                        currentPosition,
                        eventRecord(
                                record.timestamp(),
                                StreamId.streamId("input", "all"),
                                currentPosition.outputEventNumber,
                                record.eventType(),
                                record.data(),
                                record.metadata()
                        )
                ));
                return true;
            }
            return false;
        }

        private Optional<ResolvedEvent> advanceToNextEvent() {
            Optional<T> candidateOrderingValue = Optional.empty();
            Optional<ResolvedEvent> candidate = Optional.empty();
            int candidateIndex = -1;

            for (int readerIndex = 0; readerIndex < readers.length; readerIndex++) {
                EventReader reader = readers[readerIndex];
                Optional<ResolvedEvent> maybeEvent = reader.readAllForwards(currentPosition.inputPositions[readerIndex]).findFirst();
                if (maybeEvent.isPresent()) {
                    T orderingValue = mergingStrategy.toComparable(maybeEvent.get());
                    if (!candidateOrderingValue.isPresent() || orderingValue.compareTo(candidateOrderingValue.get()) < 0) {
                        candidateOrderingValue = Optional.of(orderingValue);
                        candidate = maybeEvent;
                        candidateIndex = readerIndex;
                    }
                }
            }

            if (candidate.isPresent()) {
                currentPosition = currentPosition.withNextPosition(candidateIndex, candidate.get().position());
            }
            return candidate;
        }

        @Override
        public Spliterator<ResolvedEvent> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return MAX_VALUE;
        }

        @Override
        public int characteristics() {
            return ORDERED | NONNULL | DISTINCT;
        }
    }


    @Override
    public Position emptyStorePosition() {
        Position[] positions = Stream.of(readers).map(EventReader::emptyStorePosition).collect(toList()).toArray(new Position[0]);
        return new MergedEventReaderPosition(-1L, positions);
    }

    private static class MergedEventReaderPosition implements Position {
        private final long outputEventNumber;
        private final Position[] inputPositions;

        MergedEventReaderPosition(long outputEventNumber, Position... inputPositions) {
            this.outputEventNumber = outputEventNumber;
            this.inputPositions = inputPositions;
        }

        public MergedEventReaderPosition withNextPosition(int readerIndex, Position position) {
            Position[] newPositions = inputPositions.clone();
            newPositions[readerIndex] = position;
            return new MergedEventReaderPosition(outputEventNumber + 1L, newPositions);
        }
    }

    interface MergingStrategy<T extends Comparable<T>> {
        T toComparable(ResolvedEvent event);
    }

    private static final class StreamIndexMergingStrategy implements MergingStrategy<Integer> {
        @Override
        public Integer toComparable(ResolvedEvent event) {
            return 0;
        }
    }

    private static final class EffectiveTimestampMergingStrategy implements MergingStrategy<Instant> {
        private static final Pattern EFFECTIVE_TIMESTAMP_PATTERN = Pattern.compile("\"effective_timestamp\"\\s*:\\s*\"([^\"]+)\"");

        @Override
        public Instant toComparable(ResolvedEvent event) {
            return effectiveTimestampFrom(event);
        }

        private static Instant effectiveTimestampFrom(ResolvedEvent event) {
            String metadata = new String(event.eventRecord().metadata(), UTF_8);
            Matcher matcher = EFFECTIVE_TIMESTAMP_PATTERN.matcher(metadata);
            if (matcher.find()) {
                return Instant.parse(matcher.group(1));
            }
            return Instant.MIN;
//            throw new IllegalStateException("no timestamp in metadata of " + event);
        }
    }

    public static final class MergedEventReaderPositionCodec implements PositionCodec {
        @Override
        public Position deserializePosition(String string) {
            return new MergedEventReaderPosition(-1L);
        }

        @Override
        public String serializePosition(Position position) {
            return "";
        }
    }
}
