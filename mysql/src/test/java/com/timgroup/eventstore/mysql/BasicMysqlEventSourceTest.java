package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.JavaEventStoreTest;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.tucker.info.Component;
import com.typesafe.config.Config;
import org.hamcrest.core.StringContains;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.UUID;

import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static com.typesafe.config.ConfigFactory.parseString;
import static com.typesafe.config.ConfigParseOptions.defaults;
import static com.typesafe.config.ConfigSyntax.PROPERTIES;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;

public class BasicMysqlEventSourceTest extends JavaEventStoreTest {
    static {
        try {
            DriverManager.registerDriver(new com.mysql.jdbc.Driver());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private final ConnectionProvider connectionProvider = () -> DriverManager.getConnection("jdbc:mysql://localhost:3306/sql_eventstore?useGmtMillisForDatetimes=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&useTimezone=true&serverTimezone=UTC");

    private final String tableName = "basic_eventstore";

    private final Config config = parseString(
            "hostname=localhost\n" +
                    "port=3306\n" +
                    "database=sql_eventstore\n" +
                    "username=\n" +
                    "password=\n" +
                    "driver=com.mysql.jdbc.Driver", defaults().setSyntax(PROPERTIES));

    private final BasicMysqlEventSource eventSource = BasicMysqlEventSource.pooledMasterDbEventSource(config, tableName, "test");

    @Before
    public void createTables() throws SQLException {
        BasicMysqlEventStoreSetup setup = new BasicMysqlEventStoreSetup(connectionProvider, tableName);
        setup.drop();
        setup.lazyCreate();
        setup.lazyCreate();
    }

    @Override
    public EventSource eventSource() {
        return eventSource;
    }

    @Test
    public void monitoring_provides_connection_component_containing_metadata_of_underlying_mysql_database() {
        Component onlyComponent = eventSource().monitoring().iterator().next();
        assertThat(onlyComponent.getReport().getValue().toString(),
                containsString("jdbc:mysql://localhost:3306/sql_eventstore"));

        eventSource().writeStream().write(
                streamId(randomCategory(), "the-stream-1"),
                singleton(newEvent("type-A", randomData(), randomData()))
        );

        assertThat(onlyComponent.getReport().getValue().toString(),
                containsString("jdbc:mysql://localhost:3306/sql_eventstore"));
    }

    @After
    public void closeEventSource() {
        ((BasicMysqlEventSource.PooledMysqlEventSource)eventSource).close();
    }
}