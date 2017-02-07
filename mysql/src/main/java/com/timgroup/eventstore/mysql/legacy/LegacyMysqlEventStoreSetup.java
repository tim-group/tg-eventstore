package com.timgroup.eventstore.mysql.legacy;

import com.timgroup.eventstore.mysql.ConnectionProvider;

import java.sql.*;

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

    private void create(boolean ifNotExists) {
        try (Connection connection = connectionProvider.getConnection()) {
            if (ifNotExists) {
                DatabaseMetaData meta = connection.getMetaData();
                String searchStringEscape = meta.getSearchStringEscape();
                String escapedTableName = tableName.replace("_", searchStringEscape + "_").replace("%", searchStringEscape + "%");
                try(ResultSet res = meta.getTables(null, null, escapedTableName, new String[]{"TABLE"})) {
                    if (res.first()) {
                        return;
                    }
                }
            }

            try (Statement statement = connection.createStatement()) {
                statement.execute("create table " + tableName + "(" +
                        "version bigint primary key, " +
                        "effective_timestamp datetime not null, " +
                        "eventType varchar(255) not null," +
                        "body blob not null" +
                        ")");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


}
