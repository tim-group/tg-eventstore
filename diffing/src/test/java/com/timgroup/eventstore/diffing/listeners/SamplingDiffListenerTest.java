package com.timgroup.eventstore.diffing.listeners;

import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;

import static com.timgroup.eventstore.diffing.listeners.SummarisingDiffListenerTest.diffEvent;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isEmptyOrNullString;

public final class SamplingDiffListenerTest {

    private final StringWriter similarInA = new StringWriter();
    private final StringWriter similarInB = new StringWriter();
    private final StringWriter unmatchedInA = new StringWriter();
    private final StringWriter unmatchedInB = new StringWriter();

    private final SamplingDiffListener underTest = new SamplingDiffListener(
            new PrintWriter(similarInA),
            new PrintWriter(similarInB),
            new PrintWriter(unmatchedInA),
            new PrintWriter(unmatchedInB),
            2
    );

    @Test public void
    stops_writing_samples_of_similar_events_after_maxSamplesPerCategory_has_been_reached() {
        underTest.onSimilarEvents(
                diffEvent("2017-10-01", "type1", "body1.1", 123),
                diffEvent("2017-10-01", "type1", "body1.2", 456)
        );
        underTest.onSimilarEvents(
                diffEvent("2017-10-02", "type1", "body2", 222),
                diffEvent("2017-10-02", "type2", "body2", 233)
        );
        underTest.onSimilarEvents(
                diffEvent("2017-10-03", "type2", "body3", 333),
                diffEvent("2017-11-29", "type2", "body3", 499)
        );

        assertThat(similarInA.toString(), equalTo(
                "2017-10-01\ttype1\tbody1.1\t@123<all/all/122>(type1)\n" +
                        "2017-10-02\ttype1\tbody2\t@222<all/all/221>(type1)\n"
        ));
        assertThat(similarInB.toString(), equalTo(
                "2017-10-01\ttype1\tbody1.2\t@456<all/all/455>(type1)\n" +
                        "2017-10-02\ttype2\tbody2\t@233<all/all/232>(type2)\n"
        ));
    }

    @Test public void
    stops_writing_samples_of_unmatched_events_in_streamA_after_maxSamplesPerCategory_has_been_reached() {
        underTest.onUnmatchedEventInStreamA(diffEvent("2017-01-01", "type1", "body1", 1));
        underTest.onUnmatchedEventInStreamA(diffEvent("2017-01-02", "type2", "body2", 2));
        underTest.onUnmatchedEventInStreamA(diffEvent("2017-01-03", "type3", "body3", 3));

        assertThat(unmatchedInA.toString(), equalTo(
                "2017-01-01\ttype1\tbody1\t@1<all/all/0>(type1)\n" +
                        "2017-01-02\ttype2\tbody2\t@2<all/all/1>(type2)\n"
        ));
        assertThat(unmatchedInB.toString(), isEmptyOrNullString());
    }

    @Test public void
    stops_writing_samples_of_unmatched_events_in_streamB_after_maxSamplesPerCategory_has_been_reached() {
        underTest.onUnmatchedEventInStreamB(diffEvent("2017-01-01", "type1", "body1", 1));
        underTest.onUnmatchedEventInStreamB(diffEvent("2017-01-02", "type2", "body2", 2));
        underTest.onUnmatchedEventInStreamB(diffEvent("2017-01-03", "type3", "body3", 3));

        assertThat(unmatchedInA.toString(), isEmptyOrNullString());
        assertThat(unmatchedInB.toString(), equalTo(
                "2017-01-01\ttype1\tbody1\t@1<all/all/0>(type1)\n" +
                        "2017-01-02\ttype2\tbody2\t@2<all/all/1>(type2)\n"
        ));
    }

    @Test public void
    writes_up_to_1000_samples_per_category_by_default() {
        final SamplingDiffListener underTest = new SamplingDiffListener(
                new PrintWriter(similarInA),
                new PrintWriter(similarInB),
                new PrintWriter(unmatchedInA),
                new PrintWriter(unmatchedInB)
        );
        for (int i = 0; i < 1010; i++) {
            underTest.onSimilarEvents(
                    diffEvent("2017-01-01", "type1", "body1", i),
                    diffEvent("2017-02-01", "type1", "body1", i)
            );
            underTest.onUnmatchedEventInStreamA(diffEvent("2017-03-01", "type3", "body3", i));
            underTest.onUnmatchedEventInStreamB(diffEvent("2017-04-01", "type4", "body4", i));
        }
        assertThat(numberOfLinesIn(similarInA), equalTo(1000));
        assertThat(numberOfLinesIn(similarInB), equalTo(1000));
        assertThat(numberOfLinesIn(unmatchedInA), equalTo(1000));
        assertThat(numberOfLinesIn(unmatchedInB), equalTo(1000));
    }

    private static int numberOfLinesIn(StringWriter results) {
        return results.toString().split("\n").length;
    }
}