package com.timgroup.eventstore.merging;

import com.timgroup.eventstore.api.*;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.stream.Stream;

import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singleton;

public final class EventShovel {
    private final EventReader reader;
    private final PositionCodec readerPositionCodec;
    private final EventSource output;

    public EventShovel(EventReader reader, PositionCodec readerPositionCodec, EventSource output) {
        this.reader = reader;
        this.readerPositionCodec = readerPositionCodec;
        this.output = output;
    }

    public void shovelAllNewlyAvailableEvents() {
        try (Stream<ResolvedEvent> backwardsWrittenOutputEvents = output.readAll().readAllBackwards()) {
            Optional<ResolvedEvent> maybeLastWrittenEvent = backwardsWrittenOutputEvents.findFirst();

            Position currentPosition = maybeLastWrittenEvent.map(evt -> readerPositionCodec.deserializePosition(new String(evt.eventRecord().metadata(), UTF_8))).orElse(reader.emptyStorePosition());

            reader.readAllForwards(currentPosition).forEach(evt -> {
                byte[] metadata = readerPositionCodec.serializePosition(evt.position()).getBytes(UTF_8);
                output.writeStream().write(
                        evt.eventRecord().streamId(),
                        singleton(newEvent(evt.eventRecord().eventType(), evt.eventRecord().data(), metadata))
                );
            });
        }
    }
}
