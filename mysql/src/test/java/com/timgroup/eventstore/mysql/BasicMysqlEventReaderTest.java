package com.timgroup.eventstore.mysql;

import com.codahale.metrics.MetricRegistry;
import com.timgroup.eventstore.api.ResolvedEvent;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class BasicMysqlEventReaderTest {
    private final ConnectionProvider connectionProvider = () -> DriverManager.getConnection("jdbc:mysql://localhost:3306/sql_eventstore?rewriteBatchedStatements=true&secondsBeforeRetryMaster=1&queriesBeforeRetryMaster=1&connectTimeout=5000");
    private static final String TABLE_NAME = "basic_eventstore";

    @Before
    public void setUpTable() {
        BasicMysqlEventStoreSetup setup = new BasicMysqlEventStoreSetup(connectionProvider, TABLE_NAME);
        setup.drop();
        setup.lazyCreate();
    }

    @Test
    public void interprets_event_timestamp_as_utc() throws Exception {
        System.out.println("Timezone: " + ZoneId.systemDefault());

        try (Connection conn = connectionProvider.getConnection()) {
            conn.setAutoCommit(true);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("insert into " + TABLE_NAME + "(position, timestamp, stream_category, stream_id, event_number, event_type, data, metadata)" +
                        " values(1, '2020-05-27 17:15:00', 'test', 'test', 0, 'test', '{}', '{}')");
            }
        }

        BasicMysqlEventReader eventReader = new BasicMysqlEventReader(connectionProvider, "sql_eventstore", TABLE_NAME, 100, new MetricRegistry());
        List<ResolvedEvent> allEvents = eventReader.readAllForwards().collect(toList());
        assertThat(allEvents.get(0).eventRecord().timestamp(), equalTo(Instant.parse("2020-05-27T17:15:00Z")));
    }
}
