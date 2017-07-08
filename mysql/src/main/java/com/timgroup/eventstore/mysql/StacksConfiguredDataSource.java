package com.timgroup.eventstore.mysql;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.PooledDataSource;
import com.typesafe.config.Config;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;

public final class StacksConfiguredDataSource implements AutoCloseable {

    private static Map<String, StacksConfiguredDataSource> dataSources = new ConcurrentHashMap<>();

    private final String id;
    public final PooledDataSource dataSource;

    private StacksConfiguredDataSource(String id, PooledDataSource dataSource) {
        this.id = id;
        this.dataSource = dataSource;
    }

    @Override
    public void close() {
        dataSources.remove(id);
        try {
            dataSource.close();
        } catch (SQLException e) {
            throw new RuntimeException("Unable to close data source", e);
        }
    }

    public static StacksConfiguredDataSource pooledMasterDb(Properties properties, String configPrefix) {
        String prefix = configPrefix;

        if (properties.getProperty(prefix + "hostname") == null) {
            prefix = "db." + prefix + ".";
            if (properties.getProperty(prefix) == null) {
                throw new IllegalArgumentException("unable to read configuration for data source with prefix + " + configPrefix);
            }
        }

        return pooled(
                properties.getProperty(prefix + "hostname"),
                Integer.parseInt(properties.getProperty(prefix + "port")),
                properties.getProperty(prefix + "username"),
                properties.getProperty(prefix + "password"),
                properties.getProperty(prefix + "database"),
                properties.getProperty(prefix + "driver")
        );
    }

    public static StacksConfiguredDataSource pooledReadOnlyDb(Properties properties, String configPrefix) {
        String prefix = configPrefix;

        if (properties.getProperty(prefix + "read_only_cluster") == null) {
            prefix = "db." + prefix + ".";
            if (properties.getProperty(prefix) == null) {
                throw new IllegalArgumentException("unable to read configuration for data source with prefix + " + configPrefix);
            }
        }

        return pooled(
                properties.getProperty(prefix + "read_only_cluster"),
                Integer.parseInt(properties.getProperty(prefix + "port")),
                properties.getProperty(prefix + "username"),
                properties.getProperty(prefix + "password"),
                properties.getProperty(prefix + "database"),
                properties.getProperty(prefix + "driver")
        );
    }

    public static StacksConfiguredDataSource pooledMasterDb(Config config) {
        return pooled(
                config.getString("hostname"),
                config.getInt("port"),
                config.getString("username"),
                config.getString("password"),
                config.getString("database"),
                config.getString("driver")
        );
    }

    public static StacksConfiguredDataSource pooledReadOnlyDb(Config config) {
        return pooled(
                config.getString("read_only_cluster"),
                config.getInt("port"),
                config.getString("username"),
                config.getString("password"),
                config.getString("database"),
                config.getString("driver")
        );
    }

    private static StacksConfiguredDataSource pooled(String hostname, int port, String username, String password, String database, String driver) {
        return dataSources.computeIfAbsent(idFromProperties(hostname, port, username, database, driver), id -> newPooledDataSource(id, hostname, port, username, password, database, driver));
    }

    private static String idFromProperties(String hostname, int port, String username, String database, String driver) {
        return hostname + port + username + database + driver;
    }

    private static StacksConfiguredDataSource newPooledDataSource(String id, String hostname, int port, String username, String password, String database, String driver) {
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
        return new StacksConfiguredDataSource(id, dataSource);
    }

}
