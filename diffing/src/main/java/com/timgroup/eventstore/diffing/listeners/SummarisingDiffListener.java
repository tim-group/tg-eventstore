package com.timgroup.eventstore.diffing.listeners;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.timgroup.eventstore.diffing.DiffEvent;

import java.io.PrintWriter;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

public final class SummarisingDiffListener implements DiffListener {
    private static final int DEFAULT_INTERMEDIATE_REPORT_EVENT_FREQUENCY = 250000;

    private final PrintWriter summaryWriter;
    private final int intermediateReportEventFrequency;

    private final InterestingEventsPair matching = new InterestingEventsPair();
    private final InterestingEventsPair similar = new InterestingEventsPair();
    private final InterestingEventsPair unmatched = new InterestingEventsPair();

    private int eventsProcessed = 0;
    private int lastIntermediateReportEventCount = 0;

    public SummarisingDiffListener(PrintWriter summaryWriter) {
        this(summaryWriter, DEFAULT_INTERMEDIATE_REPORT_EVENT_FREQUENCY);
    }

    public SummarisingDiffListener(PrintWriter summaryWriter, int intermediateReportEventFrequency) {
        this.summaryWriter = summaryWriter;
        this.intermediateReportEventFrequency = intermediateReportEventFrequency;
        header(Instant.now() + " starting to diff", ' ');
    }

    @Override public void onMatchingEvents(DiffEvent eventInStreamA, DiffEvent eventInStreamB) {
        matching.updateWith(eventInStreamA, eventInStreamB);
        maybePrintIntermediateReport(2);
    }

    @Override public void onSimilarEvents(DiffEvent eventInStreamA, DiffEvent eventInStreamB) {
        similar.updateWith(eventInStreamA, eventInStreamB);
        maybePrintIntermediateReport(2);
    }

    @Override public void onUnmatchedEventInStreamA(DiffEvent eventInStreamA) {
        unmatched.eventsFromA.updateWith(eventInStreamA);
        maybePrintIntermediateReport(1);
    }

    @Override public void onUnmatchedEventInStreamB(DiffEvent eventInStreamB) {
        unmatched.eventsFromB.updateWith(eventInStreamB);
        maybePrintIntermediateReport(1);
    }

    public void printFinalReport() {
        header(Instant.now() + " final diffing results after " + eventsProcessed + " processed events", '=');
        printReport();
    }

    void maybePrintIntermediateReport(int additionalEvents) {
        eventsProcessed += additionalEvents;
        if (eventsProcessed - lastIntermediateReportEventCount >= intermediateReportEventFrequency) {
            lastIntermediateReportEventCount = eventsProcessed;
            header(Instant.now() + " intermediate diffing results after " + eventsProcessed + " processed events", '=');
            printReport();
        }
    }

    void printReport() {
        section("matching pairs of events", matching.eventsFromA);
        section("similar events in stream A", similar.eventsFromA);
        section("similar events in stream B", similar.eventsFromB);
        section("unmatched events in stream A", unmatched.eventsFromA);
        section("unmatched events in stream B", unmatched.eventsFromB);
        summaryWriter.flush();
    }

    private void header(String text, Character underline) {
        summaryWriter.println();
        summaryWriter.println(text);
        summaryWriter.println(Strings.repeat(underline.toString(), text.length()));
        summaryWriter.println();
    }

    private void section(String title, InterestingEvents events) {
        if (events.totalCount > 0) {
            header(events.totalCount + " " + title, '-');
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
