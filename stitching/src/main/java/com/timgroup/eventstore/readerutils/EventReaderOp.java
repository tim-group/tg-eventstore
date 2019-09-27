package com.timgroup.eventstore.readerutils;

import com.google.common.collect.ImmutableSet;
import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.tucker.info.Component;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.Collection;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static com.timgroup.eventstore.readerutils.RekeyingEventReader.rekeying;
import static java.util.Objects.requireNonNull;

public interface EventReaderOp {
    static EventReaderOp filterContainingEventTypes(Set<String> eventTypes) {
        Set<String> s = ImmutableSet.copyOf(eventTypes);
        return r -> FilteringEventReader.containingEventTypes(r, s);
    }

    static EventReaderOp filter(Predicate<? super ResolvedEvent> predicate) {
        requireNonNull(predicate);
        return r -> new FilteringEventReader(r, predicate);
    }

    static EventReaderOp backdate(Instant cutover, Instant destination) {
        requireNonNull(cutover);
        requireNonNull(destination);
        return transformEventRecords(new BackdatingEventReader.BackdatingTransformer(cutover, destination));
    }

    static EventReaderOp rekey(StreamId streamId) {
        requireNonNull(streamId);
        return r -> rekeying(r, streamId);
    }

    static EventReaderOp excludeEventsWrittenBefore(Instant startInclusive) {
        requireNonNull(startInclusive);
        return filter(re -> !re.eventRecord().timestamp().isBefore(startInclusive));
    }

    static <T extends Comparable<T>> EventReaderOp reorder(T cutoff, Function<? super ResolvedEvent, ? extends T> sortKeyExtractor) {
        requireNonNull(cutoff);
        requireNonNull(sortKeyExtractor);
        return r -> new ReorderingEventReader<>(r, cutoff, sortKeyExtractor::apply);
    }

    static EventReaderOp transformEventRecords(UnaryOperator<EventRecord> operator) {
        requireNonNull(operator);
        return r -> TransformingEventReader.transformEventRecords(r, operator);
    }

    static EventReaderOp transformResolvedEvents(UnaryOperator<ResolvedEvent> operator) {
        requireNonNull(operator);
        return r -> TransformingEventReader.transformResolvedEvents(r, operator);
    }

    static EventReaderOp identity() {
        return r -> r;
    }

    @Nonnull
    EventReader apply(EventReader eventReader);

    @Nonnull
    default EventReader apply(EventSource eventSource) {
        return apply(eventSource.readAll());
    }

    @Nonnull
    default EventSource wrapReadOnlySource(EventSource underlying) {
        return new EventSource() {
            @Nonnull
            @Override
            public EventReader readAll() {
                return apply(underlying.readAll());
            }

            @Nonnull
            @Override
            public EventCategoryReader readCategory() {
                throw new UnsupportedOperationException();
            }

            @Nonnull
            @Override
            public EventStreamReader readStream() {
                throw new UnsupportedOperationException();
            }

            @Nonnull
            @Override
            public EventStreamWriter writeStream() {
                throw new UnsupportedOperationException();
            }

            @Nonnull
            @Override
            public Collection<Component> monitoring() {
                return underlying.monitoring();
            }
        };
    }

    default EventReaderOp andThen(EventReaderOp next) {
        return r -> next.apply(apply(r));
    }

    static EventReaderOp flow(EventReaderOp... ops) {
        if (ops.length == 0)
            return identity();
        if (ops.length == 1)
            return ops[0];
        return reader -> {
            EventReader result = reader;
            for (EventReaderOp op : ops) {
                result = op.apply(result);
            }
            return result;
        };
    }
}
