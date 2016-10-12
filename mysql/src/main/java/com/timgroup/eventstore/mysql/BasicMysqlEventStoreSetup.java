package com.timgroup.eventstore.mysql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class BasicMysqlEventStoreSetup {
    private final ConnectionProvider connectionProvider;
    private final String tableName;

    public BasicMysqlEventStoreSetup(ConnectionProvider connectionProvider, String tableName) {
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
        try (Connection connection = connectionProvider.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("create table " + tableName + "(" +
                    "position bigint primary key, " +
                    "timestamp datetime not null, " +
                    "stream_category varchar(255) not null, " +
                    "stream_id varchar(255) not null, " +
                    "event_number bigint not null, " +
                    "event_type varchar(255) not null," +
                    "data blob not null, " +
                    "metadata blob not null," +
                    "unique(stream_category, stream_id, event_number)" +
                    ")");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void lazyCreate() {
        try (Connection connection = connectionProvider.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("create table if not exists " + tableName + "(" +
                    "position bigint primary key, " +
                    "timestamp datetime not null, " +
                    "stream_category varchar(255) not null, " +
                    "stream_id varchar(255) not null, " +
                    "event_number bigint not null, " +
                    "event_type varchar(255) not null," +
                    "data blob not null, " +
                    "metadata blob not null," +
                    "unique(stream_category, stream_id, event_number)" +
                    ")");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
