package com.timgroup.eventstore.api.legacy;

import java.util.function.LongFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

import com.timgroup.eventstore.api.EventData;
import com.timgroup.eventstore.api.EventInStream;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventStore;
import com.timgroup.eventstore.api.EventStream;
import com.timgroup.eventstore.api.EventStreamJavaAdapter;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import org.joda.time.DateTimeZone;
import scala.Function1;
import scala.Option;
import scala.collection.Seq;
import scala.runtime.BoxedUnit;

public final class ReadableLegacyStore implements EventStore {
    private final EventReader eventReader;
    private final LongFunction<Position> toPosition;
    private final ToLongFunction<Position> toVersion;

    public ReadableLegacyStore(EventReader eventReader, LongFunction<Position> toPosition, ToLongFunction<Position> toVersion) {
        this.eventReader = eventReader;
        this.toPosition = toPosition;
        this.toVersion = toVersion;
    }

    @Override
    public EventStream fromAll(long version) {
        return new EventStreamJavaAdapter(eventReader.readAllForwards(toPosition.apply(version)).map(this::toEventInStream));
    }

    @Override
    public long fromAll$default$1() {
        return 0;
    }

    @Override
    public void fromAll(long version, Function1<EventInStream, BoxedUnit> eventHandler) {
        try (Stream<ResolvedEvent> stream = eventReader.readAllForwards(toPosition.apply(version))) {
            stream.map(this::toEventInStream).forEachOrdered(eventHandler::apply);
        }
    }

    @Override
    public void save(Seq<EventData> newEvents, Option<Object> expectedVersion) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Option<Object> save$default$2() {
        throw new UnsupportedOperationException();
    }

    private EventInStream toEventInStream(ResolvedEvent in) {
        return new EventInStream(
                new org.joda.time.DateTime(in.eventRecord().timestamp().toEpochMilli(), DateTimeZone.UTC),
                EventData.apply(in.eventRecord().eventType(), in.eventRecord().data()),
                toVersion.applyAsLong(in.position()));
    }

    @Override
    public String toString() {
        return eventReader.toString();
    }
}
