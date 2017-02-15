package com.timgroup.eventstore.mysql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
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
        create(false);
    }

    public void lazyCreate() {
        create(true);
    }

    private void create(boolean ifNotExists) {
        try (Connection connection = connectionProvider.getConnection()) {
            if (ifNotExists) {
                DatabaseMetaData meta = connection.getMetaData();
                String searchStringEscape = meta.getSearchStringEscape();
                String escapedTableName = tableName.replace("_", searchStringEscape + "_").replace("%", searchStringEscape + "%");
                try(ResultSet res = meta.getTables(null, null, escapedTableName, new String[]{"TABLE", "VIEW"})) {
                    if (res.first()) {
                        return;
                    }
                }
            }

            try (Statement statement = connection.createStatement()) {
                statement.execute("create table " + (ifNotExists ? "if not exists" : "") + " " + tableName + "(" +
                        "position bigint primary key, " +
                        "timestamp datetime not null, " +
                        "stream_category varchar(255) not null, " +
                        "stream_id varchar(255) not null, " +
                        "event_number bigint not null, " +
                        "event_type varchar(255) not null," +
                        "data blob not null, " +
                        "metadata blob not null," +
                        "unique(stream_category, stream_id, event_number)," +
                        "key(stream_category, position)" +
                        ")");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
