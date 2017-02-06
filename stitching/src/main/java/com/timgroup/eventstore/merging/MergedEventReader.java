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

public final class MergedEventReader implements EventReader {
    private static final Pattern EFFECTIVE_TIMESTAMP_PATTERN = Pattern.compile("\"effective_timestamp\"\\s*:\\s*\"([^\"]+)\"");

    private EventReader[] readers;

    public MergedEventReader(EventReader... readers) {
        this.readers = readers;
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        return StreamSupport.stream(new MergingSpliterator((MergedEventReaderPosition) positionExclusive, readers), false);
    }

    private static final class MergingSpliterator implements Spliterator<ResolvedEvent> {

        private final EventReader[] readers;

        private MergedEventReaderPosition currentPosition;

        private MergingSpliterator(MergedEventReaderPosition startPosition, EventReader... readers) {
            this.readers = readers;
            currentPosition = startPosition;
        }

        @Override
        public boolean tryAdvance(Consumer<? super ResolvedEvent> consumer) {
            Optional<ResolvedEvent> maybeNextInputEvent = advanceToNextEvent();
            if (maybeNextInputEvent.isPresent()) {
                ResolvedEvent re = maybeNextInputEvent.get();
                EventRecord record = re.eventRecord();
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
            Instant minimumEffectiveTimestamp = Instant.MAX;
            Optional<ResolvedEvent> candidate = Optional.empty();
            int candidateIndex = -1;

            for (int readerIndex = 0; readerIndex < readers.length; readerIndex++) {
                EventReader reader = readers[readerIndex];
                Optional<ResolvedEvent> maybeEvent = reader.readAllForwards(currentPosition.inputPositions[readerIndex]).findFirst();
                if (maybeEvent.isPresent()) {
                    Instant effectiveTimestamp = effectiveTimestampFrom(maybeEvent.get().eventRecord());
                    if (effectiveTimestamp.isBefore(minimumEffectiveTimestamp)) {
                        minimumEffectiveTimestamp = effectiveTimestamp;
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

        private static Instant effectiveTimestampFrom(EventRecord event) {
            String metadata = new String(event.metadata(), UTF_8);
            Matcher matcher = EFFECTIVE_TIMESTAMP_PATTERN.matcher(metadata);
            if (matcher.find()) {
                return Instant.parse(matcher.group(1));
            }
            return Instant.MIN;
//            throw new IllegalStateException("no timestamp in metadata of " + event);
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

    public static class MergedEventReaderPositionCodec implements PositionCodec {
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
