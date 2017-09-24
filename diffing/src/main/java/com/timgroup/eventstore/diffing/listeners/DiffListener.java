package com.timgroup.eventstore.diffing.listeners;

import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.diffing.DiffEvent;

public interface DiffListener {
    default void onMatchingEvents(DiffEvent eventInStreamA, DiffEvent eventInStreamB) {}
    default void onSimilarEvents(DiffEvent eventInStreamA, DiffEvent eventInStreamB) {}

    default void onUnmatchedEventInStreamA(DiffEvent eventInStreamA) {}
    default void onUnmatchedEventInStreamB(DiffEvent eventInStreamB) {}

    // TODO default void onProgressReportThresholdReached(int processedEvents) {}
}
