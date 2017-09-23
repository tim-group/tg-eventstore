package com.timgroup.eventstore.diffing;

import com.timgroup.eventstore.api.ResolvedEvent;

public interface DiffListener {
    default void onMatchingEvents(DiffEvent eventInStreamA, DiffEvent eventInStreamB) {}
    default void onSimilarEvents(DiffEvent eventInStreamA, DiffEvent eventInStreamB) {}

    default void onUnmatchedEventInStreamA(DiffEvent eventInStreamA) {}
    default void onUnmatchedEventInStreamB(DiffEvent eventInStreamB) {}

    // TODO default void onProgressReportThresholdReached(int processedEvents) {}
}
