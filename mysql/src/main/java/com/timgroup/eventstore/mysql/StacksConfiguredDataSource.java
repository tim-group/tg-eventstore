package com.timgroup.eventstore.mysql;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.typesafe.config.Config;

import java.util.Properties;

import static java.lang.String.format;

public class StacksConfiguredDataSource {
    public static ComboPooledDataSource pooled(Properties properties, String configPrefix) {

        return pooled(
                properties.getProperty(configPrefix + "hostname"),
                Integer.parseInt(properties.getProperty(configPrefix + "port")),
                properties.getProperty(configPrefix + "username"),
                properties.getProperty(configPrefix + "password"),
                properties.getProperty(configPrefix + "database"),
                properties.getProperty(configPrefix + "driver")
        );
    }

    public static ComboPooledDataSource pooled(Config config) {
        return pooled(
                config.getString("hostname"),
                config.getInt("port"),
                config.getString("username"),
                config.getString("password"),
                config.getString("database"),
                config.getString("driver")
        );
    }

    private static ComboPooledDataSource pooled(String hostname, int port, String username, String password, String database, String driver) {
        ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(format("jdbc:mysql://%s:%d/%s?rewriteBatchedStatements=true",
                hostname,
                port,
                database));
        dataSource.setUser(username);
        dataSource.setPassword(password);
        dataSource.setIdleConnectionTestPeriod(60 * 5);

        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return dataSource;
    }
}
