package com.timgroup.eventstore.api;

public class WrongExpectedVersion extends RuntimeException {
    private static final long serialVersionUID = 1L;
    public WrongExpectedVersion(long currentVersion, long expectedVersion) {
        super("current version: " + currentVersion + ", expected version: " + expectedVersion);
    }
}
