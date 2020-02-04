package com.timgroup.eventstore.archiver;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class ProtobufsEventIteratorTest {
    @Test
    public void extracts_multiple_messages_from_input() {
        List<EventStoreArchiverProtos.Timestamp> inputs = ImmutableList.of(asMessage(Instant.EPOCH), asMessage(Instant.parse("2020-02-04T14:27:06.123456Z")));

        ByteBuffer buffer = ByteBuffer.allocate(1024).order(LITTLE_ENDIAN);

        for (EventStoreArchiverProtos.Timestamp message : inputs) {
            buffer.putInt(message.getSerializedSize());
            buffer.put(message.toByteArray());
        }

        buffer.flip();

        List<EventStoreArchiverProtos.Timestamp> fromIterator = ImmutableList.copyOf(new ProtobufsEventIterator<>(EventStoreArchiverProtos.Timestamp.parser(), new ByteArrayInputStream(buffer.array(), 0, buffer.limit())));

        assertThat(fromIterator, equalTo(inputs));
    }

    private static EventStoreArchiverProtos.Timestamp asMessage(Instant instant) {
        return EventStoreArchiverProtos.Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
}
