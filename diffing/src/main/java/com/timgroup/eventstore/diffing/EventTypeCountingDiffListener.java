package com.timgroup.eventstore.diffing;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

public final class EventTypeCountingDiffListener implements DiffListener {
    private final PrintWriter summaryWriter;
    private final PrintWriter similarInASampleWriter;
    private final PrintWriter similarInBSampleWriter;
    private final PrintWriter unmatchedInASampleWriter;
    private final PrintWriter unmatchedInBSampleWriter;
    private final int maxSamplesPerCategory;

    private final InterestingEventsPair matching = new InterestingEventsPair();
    private final InterestingEventsPair similar = new InterestingEventsPair();
    private final InterestingEventsPair unmatched = new InterestingEventsPair();
    private final Map<String, Integer> numSamplesPerCategory = new HashMap<>();

    EventTypeCountingDiffListener(
            PrintWriter summaryWriter,
            PrintWriter similarInASampleWriter,
            PrintWriter similarInBSampleWriter,
            PrintWriter unmatchedInASampleWriter,
            PrintWriter unmatchedInBSampleWriter,
            int maxSamplesPerCategory)
    {
        this.summaryWriter = summaryWriter;
        this.similarInASampleWriter = similarInASampleWriter;
        this.similarInBSampleWriter = similarInBSampleWriter;
        this.unmatchedInASampleWriter = unmatchedInASampleWriter;
        this.unmatchedInBSampleWriter = unmatchedInBSampleWriter;
        this.maxSamplesPerCategory = maxSamplesPerCategory;
    }

    @Override public void onMatchingEvents(DiffEvent eventInStreamA, DiffEvent eventInStreamB) {
        matching.updateWith(eventInStreamA, eventInStreamB);
    }

    @Override public void onSimilarEvents(DiffEvent eventInStreamA, DiffEvent eventInStreamB) {
        similar.updateWith(eventInStreamA, eventInStreamB);
        printSample("similarInA", similarInASampleWriter, eventInStreamA);
        printSample("similarInB", similarInBSampleWriter, eventInStreamB);
    }

    @Override public void onUnmatchedEventInStreamA(DiffEvent eventInStreamA) {
        unmatched.eventsFromA.updateWith(eventInStreamA);
        printSample("unmatchedInA", unmatchedInASampleWriter, eventInStreamA);
    }

    @Override public void onUnmatchedEventInStreamB(DiffEvent eventInStreamB) {
        unmatched.eventsFromB.updateWith(eventInStreamB);
        printSample("unmatchedInB", unmatchedInBSampleWriter, eventInStreamB);
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

    private void printSample(String category, PrintWriter writer, DiffEvent event) {
        int currentSamples = numSamplesPerCategory.getOrDefault(category, 0);
        if (currentSamples < maxSamplesPerCategory) {
            writer.println(Joiner.on('\t').join(event.effectiveTimestamp, event.type, new String(event.body, UTF_8), event.underlyingEvent.locator()));
            numSamplesPerCategory.put(category, currentSamples + 1);
        }
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
