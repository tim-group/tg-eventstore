package com.timgroup.eventstore.cache;

public class CacheNotFoundException extends RuntimeException {
    public CacheNotFoundException(String reason, Exception cause) {
        super(reason, cause);
    }
}