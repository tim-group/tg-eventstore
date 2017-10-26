package com.timgroup.eventstore.diffing.listeners;

import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.diffing.DiffEvent;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public final class SummarisingDiffListenerTest {

    private final StringWriter summary = new StringWriter();
    private final SummarisingDiffListener underTest = new SummarisingDiffListener(new PrintWriter(summary));

    @Test public void
    does_not_output_empty_sections_in_report() {
        underTest.printReport();
        assertThat(summary.toString().trim(), endsWith(" starting to diff"));
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

        assertThat(summary.toString(), endsWith("\n" +
                "3 matching pairs of events\n" +
                "--------------------------\n" +
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

        assertThat(summary.toString(), endsWith("\n" +
                "3 similar events in stream A\n" +
                "----------------------------\n" +
                "per type: 2 type1, 1 type2\n" +
                "earliest: 2017-10-01 body1.1 @123<all/all/122>(type1)\n" +
                "latest:   2017-10-03 body3 @333<all/all/332>(type2)\n" +
                "\n" +
                "3 similar events in stream B\n" +
                "----------------------------\n" +
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

        assertThat(summary.toString(), endsWith("\n" +
                "2 unmatched events in stream A\n" +
                "------------------------------\n" +
                "per type: 2 type1\n" +
                "earliest: 2017-01-01 body1 @1<all/all/0>(type1)\n" +
                "latest:   2017-03-03 body3 @3<all/all/2>(type1)\n" +
                "\n" +
                "1 unmatched events in stream B\n" +
                "------------------------------\n" +
                "per type: 1 type2\n" +
                "earliest: 2017-02-02 body2 @2<all/all/1>(type2)\n" +
                "latest:   2017-02-02 body2 @2<all/all/1>(type2)\n"
        ));
    }

    @Test public void
    prints_intermediate_reports_in_configured_interval() {
        final SummarisingDiffListener listener = new SummarisingDiffListener(
                new PrintWriter(summary),
                3
        );
        listener.onMatchingEvents(
                diffEvent("2017-09-01", "type1", "body1", 1),
                diffEvent("2017-09-01", "type1", "body1", 1)
        );
        listener.onSimilarEvents(
                diffEvent("2017-09-02", "type2", "body2a", 2),
                diffEvent("2017-09-02", "type2", "body2b", 2)
        );
        listener.onUnmatchedEventInStreamA(diffEvent("2017-09-03", "type3", "body3", 3));
        listener.onMatchingEvents(
                diffEvent("2017-09-04", "type4", "body4", 4),
                diffEvent("2017-09-04", "type4", "body4", 3)
        );

        assertThat(summary.toString(), containsString("intermediate diffing results after 4 processed events"));
        assertThat(summary.toString(), containsString("intermediate diffing results after 7 processed events"));
    }

    static DiffEvent diffEvent(String timestamp, String type, String body, int position) {
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