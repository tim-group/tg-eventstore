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

            Position currentPosition = maybeLastWrittenEvent
                    .map(re -> extractShovelPositionFromMetadata(re.eventRecord().metadata()))
                    .orElse(reader.emptyStorePosition());

            reader.readAllForwards(currentPosition).forEach(evt -> {
                byte[] metadata = createMetadataWithShovelPosition(evt.position(), evt.eventRecord().metadata());
                output.writeStream().write(
                        evt.eventRecord().streamId(),
                        singleton(newEvent(evt.eventRecord().eventType(), evt.eventRecord().data(), metadata))
                );
            });
        }
    }

    /* David Ellis 03/02/2017 - Please do this properly */
    // metadata transfer from upstream -- need resolve how we combine our shovel position record with the upstream metadata
    //    from upstream we have completely unknown black box of metadata as a byte[]
    //    strategy: assume it is json -- prove by decoding
    //        if json decode fails, throw away upstream metadata and just write ours downstream
    //        if json decode passes, add our shovel_position field to it, and re-encode

    private Position extractShovelPositionFromMetadata(byte[] metadata) {
        return readerPositionCodec.deserializePosition(new String(metadata, UTF_8));
    }

    private byte[] createMetadataWithShovelPosition(Position shovelPosition, byte[] upstreamMetadata) {
        return readerPositionCodec.serializePosition(shovelPosition).getBytes(UTF_8);
    }

    //TODO:
    // batched writing
    // optimistic locking


}
