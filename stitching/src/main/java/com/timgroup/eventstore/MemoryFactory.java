package com.timgroup.eventstore;

import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.memory.InMemoryEventSource;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import com.timgroup.indicatorinputstreamwriter.plumbing.MetadataSerialiser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

public class MemoryFactory {
    final JavaInMemoryEventStore inputReader =  new JavaInMemoryEventStore(ArrayList::new, Clock.systemUTC());
    final InMemoryEventSource inputSource = new InMemoryEventSource(inputReader);

    MemoryFactory(String resource) {
        try {
            try (Stream<String> lines = new BufferedReader(new InputStreamReader(new GZIPInputStream(MemoryFactory.class.getResourceAsStream(resource))))
                    .lines()) {
                List<NewEvent> events = lines.map(line -> toNewEvent(line)).collect(Collectors.toList());
                inputSource.writeStream().write(StreamId.streamId("all","all"),events);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private NewEvent toNewEvent(String line) {
        String[] parts = line.split("\t");
        final Instant dateTime = Instant.parse(parts[1]);
        final String type = parts[2];
        final byte[] body = parts[3].getBytes();
        return NewEvent.newEvent(type, body, MetadataSerialiser.writeMetadataWithEffectiveTimestamp(dateTime));
    }

    public static EventSource fromResource(String resource) {
        return new MemoryFactory(resource).inputSource;
    }

    public PositionCodec positionCodec() {
        return inputSource.positionCodec();
    }
}