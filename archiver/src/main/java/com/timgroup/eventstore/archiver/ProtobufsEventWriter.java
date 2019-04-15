package com.timgroup.eventstore.archiver;

import com.google.protobuf.Message;

import java.nio.ByteBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

public class ProtobufsEventWriter {
    private final ByteBuffer buffer;

    public ProtobufsEventWriter(ByteBuffer buffer) {
        this.buffer = buffer.order(LITTLE_ENDIAN);
    }

    public void write(Message message) {
        byte[] bytes = message.toByteArray();
        buffer.putInt(bytes.length);
        buffer.put(bytes);
    }
}
