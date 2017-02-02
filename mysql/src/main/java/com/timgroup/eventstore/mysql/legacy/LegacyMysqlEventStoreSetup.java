package com.timgroup.eventstore.mysql.legacy;

import com.timgroup.eventstore.mysql.ConnectionProvider;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class LegacyMysqlEventStoreSetup {
    private final ConnectionProvider connectionProvider;
    private final String tableName;

    public LegacyMysqlEventStoreSetup(ConnectionProvider connectionProvider, String tableName) {
        this.connectionProvider = connectionProvider;
        this.tableName = tableName;
    }

    public void drop() {
        try (Connection connection = connectionProvider.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("drop table if exists " + tableName);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void create() {
        create(false);
    }

    public void lazyCreate() {
        create(true);
    }

    private void create(boolean drop) {
        try (Connection connection = connectionProvider.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("create table " + (drop ? "if not exists" : "") + " " + tableName + "(" +
                    "version bigint primary key, " +
                    "effective_timestamp datetime not null, " +
                    "eventType varchar(255) not null," +
                    "body blob not null" +
                    ")");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
