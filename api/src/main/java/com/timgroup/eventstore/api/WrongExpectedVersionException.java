package com.timgroup.eventstore.api;

public class WrongExpectedVersionException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public WrongExpectedVersionException(long currentVersion, long expectedVersion) {
        super("current version: " + currentVersion + ", expected version: " + expectedVersion);
    }

    public WrongExpectedVersionException(String msg) {
        super(msg);
    }
}
