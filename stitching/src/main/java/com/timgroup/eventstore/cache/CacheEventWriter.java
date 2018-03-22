package com.timgroup.eventstore.cache;

import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;

import javax.annotation.ParametersAreNonnullByDefault;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

@ParametersAreNonnullByDefault
public class CacheEventWriter implements AutoCloseable {
    private final PositionCodec positionCodec;
    private final DataOutputStream output;

    public CacheEventWriter(OutputStream outputStream, PositionCodec positionCodec) {
        this.positionCodec = requireNonNull(positionCodec);
        this.output = new DataOutputStream(requireNonNull(outputStream));
    }

    public void write(ResolvedEvent resolvedEvent) {
        try {
            output.writeUTF(positionCodec.serializePosition(resolvedEvent.position()));
            EventRecord eventRecord = resolvedEvent.eventRecord();
            output.writeLong(eventRecord.timestamp().toEpochMilli());
            StreamId streamId = eventRecord.streamId();
            output.writeUTF(streamId.category());
            output.writeUTF(streamId.id());
            output.writeLong(eventRecord.eventNumber());
            output.writeUTF(eventRecord.eventType());
            output.writeInt(eventRecord.data().length);
            output.write(eventRecord.data());
            output.writeInt(eventRecord.metadata().length);
            output.write(eventRecord.metadata());
        } catch (IOException  e) {
            throw new CacheWriteException(e);
        }
    }

    @Override
    public void close() throws Exception {
        output.close();
    }

    public static class CacheWriteException extends RuntimeException {
        public CacheWriteException(Exception e) {
            super(e);
        }
    }
}
