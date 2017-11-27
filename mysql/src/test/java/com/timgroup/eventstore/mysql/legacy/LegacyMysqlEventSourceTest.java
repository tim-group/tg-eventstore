package com.timgroup.eventstore.mysql.legacy;

import com.timgroup.eventstore.api.*;
import com.timgroup.eventstore.api.HasPropertyValueMatcher.PropertyToMatch;
import com.timgroup.eventstore.mysql.ConnectionProvider;
import com.typesafe.config.Config;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.UUID;
import java.util.stream.Stream;

import static com.timgroup.eventstore.api.EventStreamReader.EmptyStreamEventNumber;
import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.eventstore.api.ObjectPropertiesMatcher.objectWith;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static com.typesafe.config.ConfigFactory.parseString;
import static com.typesafe.config.ConfigParseOptions.defaults;
import static com.typesafe.config.ConfigSyntax.PROPERTIES;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@SuppressWarnings({"OptionalGetWithoutIsPresent", "unchecked"})
public final class LegacyMysqlEventSourceTest {
    static {
        try {
            DriverManager.registerDriver(new com.mysql.jdbc.Driver());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private final ConnectionProvider connectionProvider = () -> DriverManager.getConnection("jdbc:mysql://localhost:3306/sql_eventstore?useGmtMillisForDatetimes=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&useTimezone=true&serverTimezone=UTC");

    private final String tableName = "legacy_eventstore";

    private final Config config = parseString(
            "hostname=localhost\n" +
                    "port=3306\n" +
                    "database=sql_eventstore\n" +
                    "username=\n" +
                    "password=\n" +
                    "driver=com.mysql.jdbc.Driver", defaults().setSyntax(PROPERTIES));


    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private final String category_1 = randomCategory();

    private final StreamId stream_1 = streamId(category_1, "1");

    private final NewEvent event_1 = newEvent("type-A", randomData(), new byte[0]);
    private final NewEvent event_2 = newEvent("type-B", randomData(), new byte[0]);
    private final NewEvent event_3 = newEvent("type-C", randomData(), new byte[0]);

    private Instant timeBeforeTest;

    private final LegacyMysqlEventSource eventSource = LegacyMysqlEventSource.pooledMasterDbEventSource(config, tableName, stream_1,"test");

    @Before
    public void captureTime() {
        timeBeforeTest = Instant.now();
    }

    @Before
    public void createTables() throws SQLException {
        LegacyMysqlEventStoreSetup setup = new LegacyMysqlEventStoreSetup(connectionProvider, tableName);
        setup.drop();
        setup.lazyCreate();
        setup.lazyCreate();
    }

    private EventSource eventSource() {
        return eventSource;
    }

    @After
    public void closeEventSource() {
        ((LegacyMysqlEventSource.LegacyPooledMysqlEventSource)eventSource).close();
    }

    @Test
    public void
    can_read_written_events() {
        final NewEvent event_1 = newEvent("type-A", randomData(), "{\"effective_timestamp\":\"2015-01-01T00:12:11Z\"}".getBytes(UTF_8));
        final NewEvent event_2 = newEvent("type-B", randomData(), "{\"foo\":0,\"effective_timestamp\" : \"2016-02-01T12:32:02Z\", \"bar\":2}".getBytes(UTF_8));
        final NewEvent event_3 = newEvent("type-C", randomData(), new byte[0]);

        eventSource().writeStream().write(stream_1, asList(
                event_1, event_2, event_3
        ));

        assertThat(eventSource().readStream().readStreamForwards(stream_1).map(ResolvedEvent::eventRecord).collect(toList()), contains(
                objectWith(EventRecord::streamId, stream_1)
                        .and(EventRecord::eventNumber, 0L)
                        .and(EventRecord::eventType, event_1.type())
                        .and(EventRecord::data, event_1.data())
                        .and(EventRecord::metadata, "{\"effective_timestamp\":\"2015-01-01T00:12:11Z\"}".getBytes(UTF_8))
                        .and(EventRecord::timestamp, Instant.parse("2015-01-01T00:12:11Z")),
                objectWith(EventRecord::streamId, stream_1)
                        .and(EventRecord::eventNumber, 1L)
                        .and(EventRecord::eventType, event_2.type())
                        .and(EventRecord::data, event_2.data())
                        .and(EventRecord::metadata, "{\"effective_timestamp\":\"2016-02-01T12:32:02Z\"}".getBytes(UTF_8))
                        .and(EventRecord::timestamp, Instant.parse("2016-02-01T12:32:02Z")),
                objectWith(EventRecord::streamId, stream_1)
                        .and(EventRecord::eventNumber, 2L)
                        .and(EventRecord::eventType, event_3.type())
                        .and(EventRecord::data, event_3.data())
                        .andMatching((PropertyToMatch<EventRecord, String>) eventData -> new String(eventData.metadata(), UTF_8),
                                     startsWith("{\"effective_timestamp\":\"20"))
                        .andMatching(EventRecord::timestamp, shortlyAfter(timeBeforeTest))
        ));
    }

    @Test
    public void
    cannot_read_from_stream_after_reaching_end_despite_writing_more_events() {
        eventSource().writeStream().write(stream_1, singletonList(
                event_1
        ));

        Iterator<ResolvedEvent> it = eventSource().readStream().readStreamForwards(stream_1).iterator();
        assertThat(it.hasNext(), is(true));
        it.next();
        assertThat(it.hasNext(), is(false));

        eventSource().writeStream().write(stream_1, singletonList(
                event_2
        ));
        assertThat(it.hasNext(), is(false));
    }

    @Test
    public void
    can_read_from_specific_event_number() {
        eventSource().writeStream().write(stream_1, asList(
                event_1, event_2
        ));

        assertThat(eventSource().readStream().readStreamForwards(stream_1, 0).map(ResolvedEvent::eventRecord).collect(toList()), contains(
                objectWith(EventRecord::eventNumber, 1L)
        ));
    }

    @Test
    public void
    can_read_empty_set_of_events_from_end_of_stream() {
        eventSource().writeStream().write(stream_1, asList(
                event_1, event_2
        ));

        assertThat(eventSource().readStream().readStreamForwards(stream_1, 1).collect(toList()), hasSize(0));
    }

    @Test
    public void
    can_read_event_stream_backwards() {
        eventSource().writeStream().write(stream_1, singletonList(anEvent()));
        eventSource().writeStream().write(stream_1, singletonList(anEvent()));

        assertThat(eventSource().readStream().readStreamBackwards(stream_1).map(ResolvedEvent::eventRecord).collect(toList()), contains(
                objectWith(EventRecord::streamId, stream_1).and(EventRecord::eventNumber, 1L),
                objectWith(EventRecord::streamId, stream_1).and(EventRecord::eventNumber, 0L)
        ));
    }

    @Test
    public void
    can_continue_reading_stream_backwards_from_position() {
        eventSource().writeStream().write(stream_1, singletonList(anEvent()));
        eventSource().writeStream().write(stream_1, singletonList(anEvent()));
        eventSource().writeStream().write(stream_1, singletonList(anEvent()));
        eventSource().writeStream().write(stream_1, singletonList(anEvent()));

        assertThat(eventSource().readStream().readStreamBackwards(stream_1, 1L).map(ResolvedEvent::eventRecord).collect(toList()), contains(
                objectWith(EventRecord::streamId, stream_1).and(EventRecord::eventNumber, 0L)
        ));
    }

    @Test
    public void
    can_continue_reading_stream_backwards_from_position_at_beginning_of_stream() {
        eventSource().writeStream().write(stream_1, singletonList(event_1));
        eventSource().writeStream().write(stream_1, singletonList(event_2));
        eventSource().writeStream().write(stream_1, singletonList(event_3));

        assertThat(eventSource().readStream().readStreamBackwards(stream_1, 0L).collect(toList()), empty());
    }

    @Test
    public void
    can_read_last_event() {
        eventSource().writeStream().write(stream_1, asList(event_1));
        eventSource().writeStream().write(stream_1, asList(event_2));
        eventSource().writeStream().write(stream_1, asList(event_3));

        EventRecord eventRecord = eventSource().readAll().readLastEvent().map(ResolvedEvent::eventRecord).get();

        assertThat(eventRecord, is(objectWith(EventRecord::streamId, stream_1).and(EventRecord::eventNumber, 2L)));
    }

    @Test
    public void
    can_read_last_event_from_category() {
        eventSource().writeStream().write(stream_1, asList(event_1));
        eventSource().writeStream().write(stream_1, asList(event_2));
        eventSource().writeStream().write(stream_1, asList(event_3));

        EventRecord eventRecord = eventSource().readCategory().readLastEventInCategory(stream_1.category()).map(ResolvedEvent::eventRecord).get();
        assertThat(eventRecord, is(objectWith(EventRecord::streamId, stream_1).and(EventRecord::eventNumber, 2L)));
    }

    @Test
    public void
    can_read_last_event_from_stream() {
        eventSource().writeStream().write(stream_1, asList(event_1));
        eventSource().writeStream().write(stream_1, asList(event_2));
        eventSource().writeStream().write(stream_1, asList(event_3));

        EventRecord eventRecord = eventSource().readStream().readLastEventInStream(stream_1).map(ResolvedEvent::eventRecord).get();
        assertThat(eventRecord, is(objectWith(EventRecord::streamId, stream_1).and(EventRecord::eventNumber, 2L)));
    }


    @Test
    public void
    throws_exception_when_stream_does_not_exist_on_stream_creation() {
        EventStreamReader eventStreamReader = eventSource().readStream();

        thrown.expect(NoSuchStreamException.class);
        eventStreamReader.readStreamForwards(stream_1);
    }

    @Test
    public void
    throws_exception_when_stream_does_not_exist_on_stream_creation_with_event_number() {
        EventStreamReader eventStreamReader = eventSource().readStream();

        thrown.expect(NoSuchStreamException.class);
        eventStreamReader.readStreamForwards(stream_1, 0);
    }

    @Test
    public void
    throws_exception_when_stream_does_not_exist_on_backwards_stream_creation() {
        EventStreamReader eventStreamReader = eventSource().readStream();

        thrown.expect(NoSuchStreamException.class);
        eventStreamReader.readStreamBackwards(stream_1);
    }

    @Test
    public void
    throws_exception_when_stream_does_not_exist_on_backwards_stream_creation_with_event_number() {
        EventStreamReader eventStreamReader = eventSource().readStream();

        thrown.expect(NoSuchStreamException.class);
        eventStreamReader.readStreamBackwards(stream_1, Long.MAX_VALUE);
    }

    @Test
    public void
    can_read_all_events() {
        eventSource().writeStream().write(stream_1, singletonList(event_1));
        eventSource().writeStream().write(stream_1, singletonList(event_2));
        eventSource().writeStream().write(stream_1, singletonList(event_3));

        assertThat(eventSource().readAll().readAllForwards().map(ResolvedEvent::eventRecord).collect(toList()), contains(
                objectWith(EventRecord::streamId, stream_1).and(EventRecord::eventNumber, 0L),
                objectWith(EventRecord::streamId, stream_1).and(EventRecord::eventNumber, 1L),
                objectWith(EventRecord::streamId, stream_1).and(EventRecord::eventNumber, 2L)
        ));
    }

    @Test
    public void
    can_continue_reading_all_from_position() {
        eventSource().writeStream().write(stream_1, singletonList(event_1));
        eventSource().writeStream().write(stream_1, singletonList(event_2));
        eventSource().writeStream().write(stream_1, singletonList(event_3));

        try (Stream<ResolvedEvent> stream = eventSource().readAll().readAllForwards()) {
            Position position = stream.limit(1).reduce((a, b) -> b).get().position();

            assertThat(eventSource().readAll().readAllForwards(position).map(ResolvedEvent::eventRecord).collect(toList()), contains(
                    objectWith(EventRecord::streamId, stream_1).and(EventRecord::eventNumber, 1L),
                    objectWith(EventRecord::streamId, stream_1).and(EventRecord::eventNumber, 2L)
            ));
        }
    }

    @Test
    public void
    can_continue_reading_all_from_position_at_end_of_stream() {
        eventSource().writeStream().write(stream_1, singletonList(event_1));
        eventSource().writeStream().write(stream_1, singletonList(event_2));
        eventSource().writeStream().write(stream_1, singletonList(event_3));

        try (Stream<ResolvedEvent> stream = eventSource().readAll().readAllForwards()) {
            Position position = stream.reduce((a, b) -> b).get().position();

            assertThat(eventSource().readAll().readAllForwards(position).collect(toList()), empty());
        }
    }

    @Test
    public void
    can_read_all_events_backwards() {
        eventSource().writeStream().write(stream_1, singletonList(event_1));
        eventSource().writeStream().write(stream_1, singletonList(event_2));
        eventSource().writeStream().write(stream_1, singletonList(event_3));

        assertThat(eventSource().readAll().readAllBackwards().map(ResolvedEvent::eventRecord).collect(toList()), contains(
                objectWith(EventRecord::streamId, stream_1).and(EventRecord::eventNumber, 2L),
                objectWith(EventRecord::streamId, stream_1).and(EventRecord::eventNumber, 1L),
                objectWith(EventRecord::streamId, stream_1).and(EventRecord::eventNumber, 0L)
        ));
    }

    @Test
    public void
    can_continue_reading_all_backwards_from_position() {
        eventSource().writeStream().write(stream_1, singletonList(event_1));
        eventSource().writeStream().write(stream_1, singletonList(event_2));
        eventSource().writeStream().write(stream_1, singletonList(event_3));

        try (Stream<ResolvedEvent> stream = eventSource().readAll().readAllBackwards()) {
            Position position = stream.limit(1).reduce((a, b) -> b).get().position();

            assertThat(eventSource().readAll().readAllBackwards(position).map(ResolvedEvent::eventRecord).collect(toList()), contains(
                    objectWith(EventRecord::streamId, stream_1).and(EventRecord::eventNumber, 1L),
                    objectWith(EventRecord::streamId, stream_1).and(EventRecord::eventNumber, 0L)
            ));
        }
    }

    @Test
    public void
    can_continue_reading_all_backwards_from_position_at_beginning_of_stream() {
        eventSource().writeStream().write(stream_1, singletonList(event_1));
        eventSource().writeStream().write(stream_1, singletonList(event_2));
        eventSource().writeStream().write(stream_1, singletonList(event_3));

        try (Stream<ResolvedEvent> stream = eventSource().readAll().readAllBackwards()) {
            Position position = stream.reduce((a, b) -> b).get().position();

            assertThat(eventSource().readAll().readAllBackwards(position).collect(toList()), empty());
        }
    }

    @Test
    public void
    fails_if_expected_version_has_not_been_reached() {
        thrown.expect(WrongExpectedVersionException.class);
        eventSource().writeStream().write(stream_1, singletonList(event_2), 0);
    }

    @Test
    public void
    fails_if_expected_version_has_passed() {
        eventSource().writeStream().write(stream_1, singletonList(event_1));
        eventSource().writeStream().write(stream_1, singletonList(event_2));

        thrown.expect(WrongExpectedVersionException.class);
        eventSource().writeStream().write(stream_1, singletonList(event_3), 0);
    }

    @Test
    public void
    writes_when_expected_version_matches() {
        eventSource().writeStream().write(stream_1, singletonList(event_1));

        eventSource().writeStream().write(stream_1, singletonList(event_2), 0);

        assertThat(eventSource().readStream().readStreamForwards(stream_1).map(ResolvedEvent::eventRecord).collect(toList()), contains(
                objectWith(EventRecord::eventNumber, 0L),
                objectWith(EventRecord::eventNumber, 1L)
        ));
    }

    @Test public void
    can_write_expecting_empty_stream() {
        eventSource().writeStream().write(stream_1, singletonList(event_1), EmptyStreamEventNumber);

        assertThat(eventSource().readStream().readStreamForwards(stream_1).count(), is(1L));
    }

    @Test
    public void
    can_read_events_by_category() {
        NewEvent event1 = anEvent();
        NewEvent event4 = anEvent();
        eventSource().writeStream().write(stream_1, singletonList(event1));
        eventSource().writeStream().write(stream_1, singletonList(event4));

        assertThat(eventSource().readCategory().readCategoryForwards(category_1).map(ResolvedEvent::eventRecord).collect(toList()), contains(
                objectWith(EventRecord::streamId, stream_1),
                objectWith(EventRecord::streamId, stream_1)
        ));
    }

    @Test
    public void
    can_continue_reading_from_position_of_category() {
        NewEvent event1 = anEvent();
        NewEvent event4 = anEvent();
        eventSource().writeStream().write(stream_1, singletonList(event1));
        eventSource().writeStream().write(stream_1, singletonList(event4));

        Position position = eventSource().readCategory().readCategoryForwards(category_1).collect(toList()).get(0).position();

        assertThat(eventSource().readCategory().readCategoryForwards(category_1, position).map(ResolvedEvent::eventRecord).collect(toList()), Matchers.contains(
                objectWith(EventRecord::streamId, stream_1)
        ));
    }

    @Test
    public void
    can_continue_reading_from_position_at_end_of_category() {
        NewEvent event1 = anEvent();
        NewEvent event4 = anEvent();
        eventSource().writeStream().write(stream_1, singletonList(event1));
        eventSource().writeStream().write(stream_1, singletonList(event4));

        Position position = eventSource().readCategory().readCategoryForwards(category_1).reduce((a, b) -> b).get().position();

        assertThat(eventSource().readCategory().readCategoryForwards(category_1, position).collect(toList()), empty());
    }

    @Test
    public void
    can_read_events_backwards_by_category() {
        NewEvent event1 = anEvent();
        NewEvent event4 = anEvent();
        eventSource().writeStream().write(stream_1, singletonList(event1));
        eventSource().writeStream().write(stream_1, singletonList(event4));

        assertThat(eventSource().readCategory().readCategoryBackwards(category_1).map(ResolvedEvent::eventRecord).collect(toList()), contains(
                objectWith(EventRecord::streamId, stream_1),
                objectWith(EventRecord::streamId, stream_1)
        ));
    }

    @Test
    public void
    can_continue_reading_backwards_from_position_of_category() {
        NewEvent event1 = anEvent();
        NewEvent event4 = anEvent();
        eventSource().writeStream().write(stream_1, singletonList(event1));
        eventSource().writeStream().write(stream_1, singletonList(event4));

        Position position = eventSource().readCategory().readCategoryBackwards(category_1).collect(toList()).get(0).position();

        assertThat(eventSource().readCategory().readCategoryBackwards(category_1, position).map(ResolvedEvent::eventRecord).collect(toList()), Matchers.contains(
                objectWith(EventRecord::streamId, stream_1)
        ));
    }

    @Test
    public void
    can_continue_reading_backwards_from_position_at_beginning_of_category() {
        NewEvent event1 = anEvent();
        NewEvent event4 = anEvent();
        eventSource().writeStream().write(stream_1, singletonList(event1));
        eventSource().writeStream().write(stream_1, singletonList(event4));

        Position position = eventSource().readCategory().readCategoryBackwards(category_1).reduce((a, b) -> b).get().position();

        assertThat(eventSource().readCategory().readCategoryBackwards(category_1, position).collect(toList()), empty());
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
        return newEvent(UUID.randomUUID().toString(), randomData(), randomData());
    }

    private static String randomCategory() {
        return "stream_" + UUID.randomUUID().toString().replace("-", "");
    }

    private static byte[] randomData() {
        return ("{\n  \"value\": \"" + UUID.randomUUID() + "\"\n}").getBytes(UTF_8);
    }
}