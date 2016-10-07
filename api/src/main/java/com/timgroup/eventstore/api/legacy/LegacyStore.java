package com.timgroup.eventstore.api.legacy;

import java.util.List;
import java.util.function.LongFunction;
import java.util.function.ToLongFunction;

import com.timgroup.eventstore.api.EventData;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.OptimisticConcurrencyFailure;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.api.WrongExpectedVersion;
import scala.Option;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

public class LegacyStore extends ReadableLegacyStore {
    private static final byte[] DEFAULT_METADATA = "{}".getBytes(UTF_8);
    private final EventStreamWriter writer;
    private final StreamId streamId;

    public LegacyStore(EventReader eventReader, EventStreamWriter writer, StreamId streamId, LongFunction<Position> toPosition, ToLongFunction<Position> toVersion) {
        super(eventReader, toPosition, toVersion);
        this.writer = writer;
        this.streamId = streamId;
    }

    @Override
    public void save(scala.collection.Seq<EventData> newEvents, scala.Option<Object> expectedVersion) {
        if (expectedVersion.isDefined()) {
            try {
                writer.write(streamId, newEvents(newEvents), ((Long) expectedVersion.get()) - 1);
            } catch (WrongExpectedVersion e) {
                throw new OptimisticConcurrencyFailure(Option.apply(e));
            }
        }
        else {
            writer.write(streamId, newEvents(newEvents));
        }
    }

    private List<NewEvent> newEvents(scala.collection.Seq<EventData> newEvents) {
        return scala.collection.JavaConversions.seqAsJavaList(newEvents).stream().map(LegacyStore::toNewEvent).collect(toList());
    }

    @Override
    public scala.Option<Object> save$default$2() {
        return scala.Option.empty();
    }

    private static NewEvent toNewEvent(EventData e) {
        return NewEvent.newEvent(e.eventType(), e.body().data(), DEFAULT_METADATA);
    }
}
