package com.timgroup.eventstore.diffing.listeners;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.timgroup.eventstore.diffing.DiffEvent;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

public final class SummarisingDiffListener implements DiffListener {
    private final PrintWriter summaryWriter;

    private final InterestingEventsPair matching = new InterestingEventsPair();
    private final InterestingEventsPair similar = new InterestingEventsPair();
    private final InterestingEventsPair unmatched = new InterestingEventsPair();

    SummarisingDiffListener(PrintWriter summaryWriter) {
        this.summaryWriter = summaryWriter;
    }

    @Override public void onMatchingEvents(DiffEvent eventInStreamA, DiffEvent eventInStreamB) {
        matching.updateWith(eventInStreamA, eventInStreamB);
    }

    @Override public void onSimilarEvents(DiffEvent eventInStreamA, DiffEvent eventInStreamB) {
        similar.updateWith(eventInStreamA, eventInStreamB);
    }

    @Override public void onUnmatchedEventInStreamA(DiffEvent eventInStreamA) {
        unmatched.eventsFromA.updateWith(eventInStreamA);
    }

    @Override public void onUnmatchedEventInStreamB(DiffEvent eventInStreamB) {
        unmatched.eventsFromB.updateWith(eventInStreamB);
    }

    // TODO on thresholdReached -> printReport
    public void printReport() {
        section("matching pairs of events", matching.eventsFromA);
        section("similar events in stream A", similar.eventsFromA);
        section("similar events in stream B", similar.eventsFromB);
        section("unmatched events in stream A", unmatched.eventsFromA);
        section("unmatched events in stream B", unmatched.eventsFromB);
    }

    private void section(String title, InterestingEvents events) {
        if (events.totalCount > 0) {
            summaryWriter.println();
            summaryWriter.println(events.totalCount + " " + title);
            summaryWriter.println(Strings.repeat("-", (events.totalCount + " " + title).length()));
            summaryWriter.println();
            summaryWriter.println("per type: " +
                    Joiner.on(", ").join(events.countByType.entrySet().stream().sorted(comparing(Map.Entry::getKey))
                            .map(entry -> entry.getValue() + " " + entry.getKey()).collect(toList())));
            summaryWriter.println("earliest: " + textFor(events.earliest));
            summaryWriter.println("latest:   " + textFor(events.latest));
        }
    }

    private static String textFor(DiffEvent event) {
        return Joiner.on(' ').join(
                event.effectiveTimestamp,
                new String(event.body, UTF_8),
                event.underlyingEvent.locator()
        );
    }

    private static final class InterestingEventsPair {
        public final InterestingEvents eventsFromA = new InterestingEvents();
        public final InterestingEvents eventsFromB = new InterestingEvents();

        public void updateWith(DiffEvent eventFromA, DiffEvent eventFromB) {
            eventsFromA.updateWith(eventFromA);
            eventsFromB.updateWith(eventFromB);
        }
    }

    private static final class InterestingEvents {
        public DiffEvent earliest;
        public DiffEvent latest;
        public int totalCount = 0;
        public final Map<String, Integer> countByType = new HashMap<>();

        public void updateWith(DiffEvent event) {
            if (earliest == null) {
                earliest = event;
            }
            latest = event;
            totalCount++;
            countByType.merge(event.type, 1, (oldCount, one) -> oldCount + one);
        }
    }
}
