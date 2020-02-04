package com.timgroup.eventstore.archiver;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

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
            int oldLimit = in.pushLimit(size);
            T msg = parser.parseFrom(in);
            in.popLimit(oldLimit);
            return msg;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
