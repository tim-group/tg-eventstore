package com.timgroup.eventstore.api;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.stream.Stream;

import static com.timgroup.eventstore.api.ObjectPropertiesMatcher.objectWith;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

public abstract class JavaEventStoreTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private final StreamId stream_1 = StreamId.streamId("stream", "1");
    private final StreamId stream_2 = StreamId.streamId("stream", "2");
    private final StreamId stream_3 = StreamId.streamId("stream", "3");

    public abstract EventStreamWriter writer();

    public abstract EventStreamReader streamEventReader();

    public abstract EventReader allEventReader();

    public abstract EventCategoryReader eventByCategoryReader();

    public Instant timeBeforeTest;

    @Before
    public void captureTime() {
        timeBeforeTest = Instant.now();
    }

    @Test
    public void
    can_read_written_events() {
        writer().write(stream_1, asList(
                NewEvent.newEvent("type-A", "data-A".getBytes(), "metadata-A".getBytes()),
                NewEvent.newEvent("type-B", "data-B".getBytes(), "metadata-B".getBytes())
        ));

        assertThat(streamEventReader().readStreamForwards(stream_1).map(ResolvedEvent::eventRecord).collect(toList()), contains(
                objectWith(EventRecord::streamId, stream_1)
                        .and(EventRecord::eventNumber, 0L)
                        .and(EventRecord::eventType, "type-A")
                        .and(EventRecord::data, "data-A".getBytes())
                        .and(EventRecord::metadata, "metadata-A".getBytes())
                        .andMatching(EventRecord::timestamp, shortlyAfter(timeBeforeTest)),
                objectWith(EventRecord::streamId, stream_1)
                        .and(EventRecord::eventNumber, 1L)
                        .and(EventRecord::eventType, "type-B")
                        .and(EventRecord::data, "data-B".getBytes())
                        .and(EventRecord::metadata, "metadata-B".getBytes())
                        .andMatching(EventRecord::timestamp, shortlyAfter(timeBeforeTest))
        ));
    }

    @Test
    public void
    can_read_and_write_to_streams_independently() {
        writer().write(stream_1, asList(NewEvent.newEvent("type-A", "data-A".getBytes(), "metadata-A".getBytes())));
        writer().write(stream_2, asList(NewEvent.newEvent("type-B", "data-B".getBytes(), "metadata-B".getBytes())));

        assertThat(streamEventReader().readStreamForwards(stream_1).map(ResolvedEvent::eventRecord).collect(toList()), contains(
                objectWith(EventRecord::eventNumber, 0L).and(EventRecord::streamId, stream_1)
        ));
        assertThat(streamEventReader().readStreamForwards(stream_2).map(ResolvedEvent::eventRecord).collect(toList()), contains(
                objectWith(EventRecord::eventNumber, 0L).and(EventRecord::streamId, stream_2)
        ));
    }

    @Test
    public void
    can_read_from_specific_event_number() {
        writer().write(stream_1, asList(
                NewEvent.newEvent("type-A", "data-A".getBytes(), "metadata-A".getBytes()),
                NewEvent.newEvent("type-B", "data-B".getBytes(), "metadata-B".getBytes())
        ));

        assertThat(streamEventReader().readStreamForwards(stream_1, 0).map(ResolvedEvent::eventRecord).collect(toList()), contains(
                objectWith(EventRecord::eventNumber, 1L)
        ));
    }

    @Test
    public void
    can_read_all_events() {
        writer().write(stream_1, asList(NewEvent.newEvent("type-A", "data-A".getBytes(), "metadata-A".getBytes())));
        writer().write(stream_2, asList(NewEvent.newEvent("type-B", "data-B".getBytes(), "metadata-B".getBytes())));
        writer().write(stream_3, asList(NewEvent.newEvent("type-C", "data-C".getBytes(), "metadata-C".getBytes())));

        assertThat(allEventReader().readAllForwards().map(ResolvedEvent::eventRecord).collect(toList()), contains(
                objectWith(EventRecord::streamId, stream_1).and(EventRecord::eventNumber, 0L),
                objectWith(EventRecord::streamId, stream_2).and(EventRecord::eventNumber, 0L),
                objectWith(EventRecord::streamId, stream_3).and(EventRecord::eventNumber, 0L)
        ));
    }

    @Test
    public void
    can_continue_reading_from_position() {
        writer().write(stream_1, asList(NewEvent.newEvent("type-A", "data-A".getBytes(), "metadata-A".getBytes())));
        writer().write(stream_2, asList(NewEvent.newEvent("type-B", "data-B".getBytes(), "metadata-B".getBytes())));
        writer().write(stream_3, asList(NewEvent.newEvent("type-C", "data-C".getBytes(), "metadata-C".getBytes())));

        try (Stream<ResolvedEvent> stream = allEventReader().readAllForwards()) {
            Position position = stream.limit(1).reduce((a, b) -> b).get().position();

            assertThat(allEventReader().readAllForwards(position).map(ResolvedEvent::eventRecord).collect(toList()), contains(
                    objectWith(EventRecord::streamId, stream_2).and(EventRecord::eventNumber, 0L),
                    objectWith(EventRecord::streamId, stream_3).and(EventRecord::eventNumber, 0L)
            ));
        }
    }

    @Test
    public void
    fails_if_expected_version_has_not_been_reached() {
        thrown.expect(WrongExpectedVersion.class);
        writer().write(stream_1, asList(NewEvent.newEvent("type-B", "data-B".getBytes(), "metadata-B".getBytes())), 0);
    }

    @Test
    public void
    fails_if_expected_version_has_passed() {
        writer().write(stream_1, asList(NewEvent.newEvent("type-A", "data-A".getBytes(), "metadata-A".getBytes())));
        writer().write(stream_1, asList(NewEvent.newEvent("type-B", "data-A".getBytes(), "metadata-A".getBytes())));

        thrown.expect(WrongExpectedVersion.class);
        writer().write(stream_1, asList(NewEvent.newEvent("type-B", "data-B".getBytes(), "metadata-B".getBytes())), 0);
    }

    @Test
    public void
    writes_when_expected_version_matches() {
        writer().write(stream_1, asList(NewEvent.newEvent("type-A", "data-A".getBytes(), "metadata-A".getBytes())));

        writer().write(stream_1, asList(NewEvent.newEvent("type-B", "data-B".getBytes(), "metadata-B".getBytes())), 0);

        assertThat(streamEventReader().readStreamForwards(stream_1).map(ResolvedEvent::eventRecord).collect(toList()), contains(
                objectWith(EventRecord::eventNumber, 0L),
                objectWith(EventRecord::eventNumber, 1L)
        ));
    }

    @Test
    public void
    can_read_events_by_category() {
        NewEvent event1 = anEvent();
        NewEvent event4 = anEvent();
        writer().write(StreamId.streamId("Cat1", "Id1"), asList(event1));
        writer().write(StreamId.streamId("Cat3", "Id1"), asList(anEvent()));
        writer().write(StreamId.streamId("Cat2", "Id1"), asList(anEvent()));
        writer().write(StreamId.streamId("Cat1", "Id2"), asList(event4));

        assertThat(eventByCategoryReader().readCategoryForwards("Cat1").map(ResolvedEvent::eventRecord).collect(toList()), contains(
                objectWith(EventRecord::streamId, StreamId.streamId("Cat1", "Id1")),
                objectWith(EventRecord::streamId, StreamId.streamId("Cat1", "Id2"))
        ));
    }

    @Test
    public void
    can_continue_reading_from_position_of_category() {
        NewEvent event1 = anEvent();
        NewEvent event4 = anEvent();
        writer().write(StreamId.streamId("Cat1", "Id1"), asList(event1));
        writer().write(StreamId.streamId("Cat3", "Id1"), asList(anEvent()));
        writer().write(StreamId.streamId("Cat2", "Id1"), asList(anEvent()));
        writer().write(StreamId.streamId("Cat1", "Id2"), asList(event4));

        Position position = eventByCategoryReader().readCategoryForwards("Cat1").collect(toList()).get(0).position();

        assertThat(eventByCategoryReader().readCategoryForwards("Cat1", position).map(ResolvedEvent::eventRecord).collect(toList()), Matchers.contains(
                objectWith(EventRecord::streamId, StreamId.streamId("Cat1", "Id2"))
        ));
    }

    private static Matcher<Instant> shortlyAfter(Instant expected) {
        return new TypeSafeDiagnosingMatcher<Instant>() {
            @Override
            protected boolean matchesSafely(Instant instant, Description description) {
                long seconds = Duration.between(expected, instant).getSeconds();

                description.appendText(" got " + instant);

                return seconds < 1;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("at most 1s after " + expected);
            }
        };
    }

    private static NewEvent anEvent() {
        return NewEvent.newEvent(UUID.randomUUID().toString(), UUID.randomUUID().toString().getBytes(), UUID.randomUUID().toString().getBytes());
    }
}