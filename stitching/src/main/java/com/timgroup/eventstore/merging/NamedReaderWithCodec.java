package com.timgroup.eventstore.merging;

import com.google.common.base.Preconditions;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.PositionCodec;

public final class NamedReaderWithCodec {
    final String name;
    final EventReader reader;
    final PositionCodec codec;

    public NamedReaderWithCodec(String name, EventReader reader, PositionCodec codec) {
        Preconditions.checkNotNull(name, "name cannot be null");
        Preconditions.checkArgument(!name.isEmpty(), "name cannot be empty");

        this.name = name;
        this.reader = reader;
        this.codec = codec;
    }

    public static NamedReaderWithCodec fromEventSource(String name, EventSource source) {
        return new NamedReaderWithCodec(name, source.readAll(), source.positionCodec());
    }

    EventReader toReader() {
        return reader;
    }

    @Override
    public String toString() {
        return "NamedReaderWithCodec{" +
                "name='" + name + '\'' +
                ", reader=" + reader +
                ", codec=" + codec +
                '}';
    }
}
