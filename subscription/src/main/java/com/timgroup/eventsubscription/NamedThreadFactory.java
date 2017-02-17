package com.timgroup.eventsubscription;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {
    private final AtomicInteger count = new AtomicInteger();
    private final String name;

    public NamedThreadFactory(String name) {
        this.name = name;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, name + "-" + count.getAndIncrement());
        thread.setDaemon(false);
        return thread;
    }
}
