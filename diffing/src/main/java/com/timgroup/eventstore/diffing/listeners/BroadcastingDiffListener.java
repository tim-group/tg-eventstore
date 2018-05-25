package com.timgroup.eventstore.diffing.listeners;

import com.timgroup.eventstore.diffing.DiffEvent;

import java.util.Arrays;
import java.util.List;

public class BroadcastingDiffListener implements DiffListener {

    private final List<DiffListener> listeners;

    public BroadcastingDiffListener(DiffListener... listeners) {
        this.listeners = Arrays.asList(listeners);
    }

    @Override public void onMatchingEvents(DiffEvent eventInStreamA, DiffEvent eventInStreamB) {
        listeners.forEach(l -> l.onMatchingEvents(eventInStreamA, eventInStreamB));
    }

    @Override public void onSimilarEvents(DiffEvent eventInStreamA, DiffEvent eventInStreamB) {
        listeners.forEach(l -> l.onSimilarEvents(eventInStreamA, eventInStreamB));
    }

    @Override public void onUnmatchedEventInStreamA(DiffEvent eventInStreamA) {
        listeners.forEach(l -> l.onUnmatchedEventInStreamA(eventInStreamA));
    }

    @Override public void onUnmatchedEventInStreamB(DiffEvent eventInStreamB) {
        listeners.forEach(l -> l.onUnmatchedEventInStreamB(eventInStreamB));
    }
}
