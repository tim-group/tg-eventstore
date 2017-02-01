package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.JavaEventStoreTest;
import com.typesafe.config.Config;
import org.junit.After;
import org.junit.Before;

import java.sql.DriverManager;
import java.sql.SQLException;

import static com.typesafe.config.ConfigFactory.parseString;
import static com.typesafe.config.ConfigParseOptions.defaults;
import static com.typesafe.config.ConfigSyntax.PROPERTIES;

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

    @After
    public void closeEventSource() {
        ((BasicMysqlEventSource.PooledMysqlEventSource)eventSource).close();
    }
}