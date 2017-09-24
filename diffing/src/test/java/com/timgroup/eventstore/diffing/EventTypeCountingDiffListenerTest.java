package com.timgroup.eventstore.diffing;

import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isEmptyOrNullString;

public final class EventTypeCountingDiffListenerTest {

    private final StringWriter summary = new StringWriter();
    private final StringWriter similarInA = new StringWriter();
    private final StringWriter similarInB = new StringWriter();
    private final StringWriter unmatchedInA = new StringWriter();
    private final StringWriter unmatchedInB = new StringWriter();

    private final EventTypeCountingDiffListener underTest = new EventTypeCountingDiffListener(
            new PrintWriter(summary),
            new PrintWriter(similarInA),
            new PrintWriter(similarInB),
            new PrintWriter(unmatchedInA),
            new PrintWriter(unmatchedInB),
            2
    );

    @Test public void
    does_not_output_empty_sections_in_report() {
        underTest.printReport();

        assertThat(summary.toString(), isEmptyOrNullString());
   }

    @Test public void
    reports_matching_events() {
        underTest.onMatchingEvents(
                diffEvent("2017-09-01", "type1", "body1", 123),
                diffEvent("2017-09-01", "type1", "body1", 456)
        );
        underTest.onMatchingEvents(
                diffEvent("2017-09-02", "type2", "body2", 222),
                diffEvent("2017-09-02", "type2", "body2", 233)
        );
        underTest.onMatchingEvents(
                diffEvent("2017-09-03", "type2", "body3", 333),
                diffEvent("2017-09-03", "type2", "body3", 499)
        );

        underTest.printReport();

        assertThat(summary.toString(), equalTo("\n" +
                "3 matching pairs of events\n" +
                "--------------------------\n" +
                "\n" +
                "per type: 1 type1, 2 type2\n" +
                "earliest: 2017-09-01 body1 @123<all/all/122>(type1)\n" +
                "latest:   2017-09-03 body3 @333<all/all/332>(type2)\n"
        ));
    }

    @Test public void
    reports_similar_events() {
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

        underTest.printReport();

        assertThat(summary.toString(), equalTo("\n" +
                "3 similar events in stream A\n" +
                "----------------------------\n" +
                "\n" +
                "per type: 2 type1, 1 type2\n" +
                "earliest: 2017-10-01 body1.1 @123<all/all/122>(type1)\n" +
                "latest:   2017-10-03 body3 @333<all/all/332>(type2)\n" +
                "\n" +
                "3 similar events in stream B\n" +
                "----------------------------\n" +
                "\n" +
                "per type: 1 type1, 2 type2\n" +
                "earliest: 2017-10-01 body1.2 @456<all/all/455>(type1)\n" +
                "latest:   2017-11-29 body3 @499<all/all/498>(type2)\n"
        ));
    }

    @Test public void
    reports_unmatched_events() {
        underTest.onUnmatchedEventInStreamA(diffEvent("2017-01-01", "type1", "body1", 1));
        underTest.onUnmatchedEventInStreamB(diffEvent("2017-02-02", "type2", "body2", 2));
        underTest.onUnmatchedEventInStreamA(diffEvent("2017-03-03", "type1", "body3", 3));

        underTest.printReport();

        assertThat(summary.toString(), equalTo("\n" +
                "2 unmatched events in stream A\n" +
                "------------------------------\n" +
                "\n" +
                "per type: 2 type1\n" +
                "earliest: 2017-01-01 body1 @1<all/all/0>(type1)\n" +
                "latest:   2017-03-03 body3 @3<all/all/2>(type1)\n" +
                "\n" +
                "1 unmatched events in stream B\n" +
                "------------------------------\n" +
                "\n" +
                "per type: 1 type2\n" +
                "earliest: 2017-02-02 body2 @2<all/all/1>(type2)\n" +
                "latest:   2017-02-02 body2 @2<all/all/1>(type2)\n"
        ));
    }

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

    private static DiffEvent diffEvent(String timestamp, String type, String body, int position) {
        EventRecord eventRecord = eventRecord(
                Instant.now(),
                streamId("all", "all"),
                position - 1,
                type,
                body.getBytes(UTF_8),
                ("{\"effective_timestamp\":\"" + timestamp + "\"}").getBytes(UTF_8)
        );
        return DiffEvent.from(new ResolvedEvent(new Pos(position), eventRecord));
    }

    private static final class Pos implements Position {
        private final int index;
        Pos(int index) {
            this.index = index;
        }
        @Override public String toString() {
            return Integer.toString(index);
        }
    }
}