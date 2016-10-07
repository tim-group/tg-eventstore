package com.timgroup.eventsubscription.healthcheck;

import java.util.OptionalLong;

public interface SubscriptionListener {
    void caughtUpAt(long version);

    void staleAtVersion(OptionalLong version);

    void terminated(long version, Exception e);
}
