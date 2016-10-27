package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.JavaEventStoreTest;
import org.junit.Before;

import java.sql.DriverManager;
import java.sql.SQLException;

public class BasicMysqlEventStoreTest extends JavaEventStoreTest {
    static {
        try {
            DriverManager.registerDriver(new com.mysql.jdbc.Driver());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private final ConnectionProvider connectionProvider = () -> DriverManager.getConnection("jdbc:mysql://localhost:3306/sql_eventstore?useGmtMillisForDatetimes=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&useTimezone=true&serverTimezone=UTC");

    private final String tableName = "basic_eventstore";

    @Before
    public void createTables() throws SQLException {
        BasicMysqlEventStoreSetup setup = new BasicMysqlEventStoreSetup(connectionProvider, tableName);
        setup.drop();
        setup.lazyCreate();
        setup.lazyCreate();
    }

    @Override
    public EventSource eventSource() {
        return new BasicMysqlEventSource(connectionProvider, tableName, 1);
    }
}