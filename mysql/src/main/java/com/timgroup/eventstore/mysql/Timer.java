package com.timgroup.eventstore.mysql;

public interface Timer {
    void time(Runnable r);
}
