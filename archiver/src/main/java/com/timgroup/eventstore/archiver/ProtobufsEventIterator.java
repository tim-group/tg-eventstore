package com.timgroup.eventstore.archiver;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

public class ProtobufsEventIterator<T extends Message> implements Iterator<T> {
    private final CodedInputStream in;
    private final Parser<? extends T> parser;

    public ProtobufsEventIterator(Parser<? extends T> parser, InputStream inputStream) {
        this.parser = parser;
        in = CodedInputStream.newInstance(inputStream);
    }

    @Override
    public boolean hasNext() {
        try {
            return !in.isAtEnd();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T next() {
        try {
            int size = in.readFixed32();
            T msg = parser.parseFrom(in.readRawBytes(size));
            in.resetSizeCounter();
            return msg;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    // Replace with com.google.protobuf.Parser when on a version >= 2.6
    interface Parser<MessageType> {
        MessageType parseFrom(byte[] inputStream) throws InvalidProtocolBufferException;
    }
}
