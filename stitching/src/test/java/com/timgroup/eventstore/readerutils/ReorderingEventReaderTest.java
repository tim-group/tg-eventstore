package com.timgroup.eventstore.readerutils;

import com.timgroup.clocks.testing.ManualClock;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static com.google.common.collect.Iterables.getLast;
import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.eventstore.api.EventRecordMatcher.anEventRecord;
import static java.time.ZoneId.systemDefault;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public final class ReorderingEventReaderTest {

    private final ManualClock clock = new ManualClock(Instant.parse("2017-01-02T12:00:00Z"), systemDefault());
    private final JavaInMemoryEventStore underlying = new JavaInMemoryEventStore(clock);

    private final String liveCutoffEventType = "AnEventCutoff";
    private final StreamId aStream = StreamId.streamId("all", "all");

    private final ReorderingEventReader<String> reader = new ReorderingEventReader<>(underlying, liveCutoffEventType, re -> re.eventRecord().eventType());

    @Test
    public void emits_nothing_when_there_are_no_events_on_or_after_the_cutoff() {
        underlying.write(aStream, singletonList(newEvent("AnEvent", new byte[0], new byte[0])));

        assertThat(reader.readAllForwards().map(ResolvedEvent::eventRecord).collect(toList()), is(empty()));
    }

    @Test
    public void buffers_events_before_the_cutoff_and_reorders_them_before_emitting() {
        underlying.write(aStream, Arrays.asList(
                newEvent("AnEvent2", new byte[0], new byte[0]),
                newEvent("AnEvent1", new byte[0], new byte[0]),
                newEvent("AnEventCutoff", new byte[0], new byte[0])
                ));

        assertThat(reader.readAllForwards().map(ResolvedEvent::eventRecord).collect(toList()),
                contains(
                        anEventRecord(
                                clock.instant(),
                                aStream,
                                1L,
                                "AnEvent1",
                                new byte[0],
                                new byte[0]
                        ),
                        anEventRecord(
                                clock.instant(),
                                aStream,
                                0L,
                                "AnEvent2",
                                new byte[0],
                                new byte[0]
                        ),
                        anEventRecord(
                                clock.instant(),
                                aStream,
                                2L,
                                "AnEventCutoff",
                                new byte[0],
                                new byte[0]
                        )
                )
        );
    }

    @Test
    public void can_resume_from_a_recorded_position() {
        underlying.write(aStream, Arrays.asList(
                newEvent("AnEvent3", new byte[0], new byte[0]),
                newEvent("AnEvent2", new byte[0], new byte[0]),
                newEvent("AnEvent1", new byte[0], new byte[0]),
                newEvent("AnEventCutoff", new byte[0], new byte[0])
        ));

        List<ResolvedEvent> firstBatch = reader.readAllForwards().limit(2).collect(toList());
        assertThat(firstBatch.stream().map(re -> re.eventRecord().eventType()).collect(toList()),
                contains("AnEvent1", "AnEvent2"));

        Position position = getLast(firstBatch).position();
        List<ResolvedEvent> secondBatch = reader.readAllForwards(position).collect(toList());
        assertThat(secondBatch.stream().map(re -> re.eventRecord().eventType()).collect(toList()),
                contains("AnEvent3", "AnEventCutoff"));
    }

}