package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.EventStreamWriter;
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
    public EventStreamWriter writer() {
        return new BasicMysqlEventStreamWriter(connectionProvider, tableName);
    }

    @Override
    public EventStreamReader streamEventReader() {
        return new BasicMysqlEventStreamReader(connectionProvider, tableName);
    }

    @Override
    public EventReader allEventReader() {
        return new BasicMysqlEventReader(connectionProvider, tableName);
    }

    @Override
    public EventCategoryReader eventByCategoryReader() {
        return new BasicMysqlEventCategoryReader(connectionProvider, tableName);
    }
}