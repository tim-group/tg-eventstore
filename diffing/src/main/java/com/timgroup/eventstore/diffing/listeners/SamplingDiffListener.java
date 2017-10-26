package com.timgroup.eventstore.diffing.listeners;

import com.google.common.base.Joiner;
import com.timgroup.eventstore.diffing.DiffEvent;

import java.io.PrintWriter;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class SamplingDiffListener implements DiffListener {
    private final PrintWriter similarInASampleWriter;
    private final PrintWriter similarInBSampleWriter;
    private final PrintWriter unmatchedInASampleWriter;
    private final PrintWriter unmatchedInBSampleWriter;
    private final int maxSamplesPerCategory;

    private int similarInASamples = 0;
    private int similarInBSamples = 0;
    private int unmatchedInASamples = 0;
    private int unmatchedInBSamples = 0;

    public SamplingDiffListener(
            PrintWriter similarInASampleWriter,
            PrintWriter similarInBSampleWriter,
            PrintWriter unmatchedInASampleWriter,
            PrintWriter unmatchedInBSampleWriter,
            int maxSamplesPerCategory)
    {
        this.similarInASampleWriter = similarInASampleWriter;
        this.similarInBSampleWriter = similarInBSampleWriter;
        this.unmatchedInASampleWriter = unmatchedInASampleWriter;
        this.unmatchedInBSampleWriter = unmatchedInBSampleWriter;
        this.maxSamplesPerCategory = maxSamplesPerCategory;
    }

    public SamplingDiffListener(
            PrintWriter similarInASampleWriter,
            PrintWriter similarInBSampleWriter,
            PrintWriter unmatchedInASampleWriter,
            PrintWriter unmatchedInBSampleWriter)
    {
        this(similarInASampleWriter, similarInBSampleWriter, unmatchedInASampleWriter, unmatchedInBSampleWriter, 1000);
    }

    @Override public void onSimilarEvents(DiffEvent eventInStreamA, DiffEvent eventInStreamB) {
        maybePrintSample(similarInASamples++, similarInASampleWriter, eventInStreamA);
        maybePrintSample(similarInBSamples++, similarInBSampleWriter, eventInStreamB);
    }

    @Override public void onUnmatchedEventInStreamA(DiffEvent eventInStreamA) {
        maybePrintSample(unmatchedInASamples++, unmatchedInASampleWriter, eventInStreamA);
    }

    @Override public void onUnmatchedEventInStreamB(DiffEvent eventInStreamB) {
        maybePrintSample(unmatchedInBSamples++, unmatchedInBSampleWriter, eventInStreamB);
    }

    private void maybePrintSample(int currentSamples, PrintWriter writer, DiffEvent event) {
        if (currentSamples < maxSamplesPerCategory) {
            writer.println(Joiner.on('\t').join(event.effectiveTimestamp, event.type, new String(event.body, UTF_8), event.underlyingEvent.locator()));
        }
    }
}
