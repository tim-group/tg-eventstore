package com.timgroup.eventstore.mysql;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.timgroup.eventstore.api.ResolvedEvent;
import io.prometheus.client.CollectorRegistry;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.SortedMap;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class BasicMysqlEventReaderTest {
    private final ConnectionProvider connectionProvider = () -> DriverManager.getConnection("jdbc:mysql://localhost:3306/sql_eventstore?rewriteBatchedStatements=true&secondsBeforeRetryMaster=1&queriesBeforeRetryMaster=1&connectTimeout=5000");
    private static final String TABLE_NAME = "basic_eventstore";

    @Before
    public void setUpTable() throws SQLException {
        BasicMysqlEventStoreSetup setup = new BasicMysqlEventStoreSetup(connectionProvider, TABLE_NAME);
        setup.drop();
        setup.lazyCreate();

        try (Connection conn = connectionProvider.getConnection()) {
            conn.setAutoCommit(true);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("insert into " + TABLE_NAME + "(position, timestamp, stream_category, stream_id, event_number, event_type, data, metadata)" +
                        " values(1, '2020-05-27 17:15:00', 'test', 'test', 0, 'test', '{}', '{}')");
            }
        }
    }

    @Test
    public void interprets_event_timestamp_as_utc() throws Exception {
        BasicMysqlEventReader eventReader = new BasicMysqlEventReader(connectionProvider, "sql_eventstore", TABLE_NAME, 100, new MetricRegistry());
        List<ResolvedEvent> allEvents = eventReader.readAllForwards().collect(toList());
        assertThat(allEvents.get(0).eventRecord().timestamp(), equalTo(Instant.parse("2020-05-27T17:15:00Z")));
    }

    @Test
    public void produces_dropwizard_metrics() {
        MetricRegistry metricRegistry = new MetricRegistry();
        BasicMysqlEventReader eventReader = new BasicMysqlEventReader(connectionProvider, "sql_eventstore", TABLE_NAME, 100, metricRegistry);
        eventReader.readAllForwards().collect(toList());

        Double prometheusMetric = CollectorRegistry.defaultRegistry.getSampleValue("tg_eventstore_page_fetch_seconds_count", new String[]{"database", "table"}, new String[]{"sql_eventstore", TABLE_NAME});
        assertThat(prometheusMetric, is(nullValue()));

        SortedMap<String, Timer> expectedMetric = metricRegistry.getTimers((name, metric) -> name.equals("database.sql_eventstore." + TABLE_NAME + ".read_all.page_fetch_time"));
        assertThat(expectedMetric.keySet(), hasSize(1));
        assertThat(expectedMetric.get(expectedMetric.firstKey()).getCount(), is(greaterThan(0L)));
    }

    @Test
    public void produces_prometheus_metrics() {
        MetricRegistry metricRegistry = new MetricRegistry();
        BasicMysqlEventReader eventReader = new BasicMysqlEventReader(connectionProvider, "sql_eventstore", TABLE_NAME, 100, null);
        eventReader.readAllForwards().collect(toList());

        Double prometheusMetric = CollectorRegistry.defaultRegistry.getSampleValue("tg_eventstore_page_fetch_seconds_count",
                new String[]{"database", "table", "read_type"},
                new String[]{"sql_eventstore", TABLE_NAME, "read_all"});
        assertThat(prometheusMetric, is(not(nullValue())));

        SortedMap<String, Timer> expectedMetric = metricRegistry.getTimers((name, metric) -> name.equals("database.sql_eventstore." + TABLE_NAME + ".read_all.page_fetch_time"));
        assertThat(expectedMetric.keySet(), hasSize(0));
    }
}
