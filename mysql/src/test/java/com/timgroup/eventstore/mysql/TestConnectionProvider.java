package com.timgroup.eventstore.mysql;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public final class TestConnectionProvider implements ConnectionProvider {
    public static ConnectionProvider create() {
        return new TestConnectionProvider();
    }

    @Nonnull
    @Override
    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:mysql://localhost:3306/sql_eventstore?useSSL=false&rewriteBatchedStatements=true&secondsBeforeRetryMaster=1&queriesBeforeRetryMaster=1&connectTimeout=5000");
    }
}
