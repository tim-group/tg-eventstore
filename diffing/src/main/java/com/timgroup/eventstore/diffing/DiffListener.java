package com.timgroup.eventstore.diffing;

import com.timgroup.eventstore.api.ResolvedEvent;

public interface DiffListener {
    default void onMatchingEvents(ResolvedEvent eventInStreamA, ResolvedEvent eventInStreamB) {}
    default void onDifferingEvents(ResolvedEvent eventInStreamA, ResolvedEvent eventInStreamB) {}

    default void onUnmatchedEventInStreamA(ResolvedEvent eventInStreamA) {}
    default void onUnmatchedEventInStreamB(ResolvedEvent eventInStreamB) {}

    // TODO default void onProgressReportThresholdReached(int processedEvents) {}
}
