package com.timgroup.eventstore.merging;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.PositionCodec;

public final class NamedReaderWithCodec {
    final String name;
    final EventReader reader;
    final PositionCodec codec;

    public NamedReaderWithCodec(String name, EventReader reader, PositionCodec codec) {
        this.name = name;
        this.reader = reader;
        this.codec = codec;
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
