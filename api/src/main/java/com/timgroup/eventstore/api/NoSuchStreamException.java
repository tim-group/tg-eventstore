package com.timgroup.eventstore.api;

public class NoSuchStreamException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    public NoSuchStreamException(StreamId streamId) {
        super("stream does not exist: " + streamId);
    }
}
