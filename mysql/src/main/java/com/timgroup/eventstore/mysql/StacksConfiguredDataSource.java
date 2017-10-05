package com.timgroup.eventstore.mysql;

import java.util.Properties;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.PooledDataSource;
import com.typesafe.config.Config;

import static java.lang.String.format;

public final class StacksConfiguredDataSource {

    public static final int DEFAULT_MAX_POOLSIZE = 15;

    private StacksConfiguredDataSource() { /* prevent instantiation */ }

    public static PooledDataSource pooledMasterDb(Properties properties, String configPrefix) {
        return pooledMasterDb(properties, configPrefix, DEFAULT_MAX_POOLSIZE);
    }

    public static PooledDataSource pooledMasterDb(Properties properties, String configPrefix, int maxPoolSize) {
        String hostname = properties.getProperty(configPrefix + "hostname");

        if (hostname == null) {
            throw new IllegalArgumentException("No " + configPrefix + "hostname property available to configure data source");
        }

        return pooled(
                hostname,
                Integer.parseInt(properties.getProperty(configPrefix + "port")),
                properties.getProperty(configPrefix + "username"),
                properties.getProperty(configPrefix + "password"),
                properties.getProperty(configPrefix + "database"),
                properties.getProperty(configPrefix + "driver"),
                maxPoolSize
        );
    }

    public static PooledDataSource pooledReadOnlyDb(Properties properties, String configPrefix) {
        return pooledReadOnlyDb(properties, configPrefix, DEFAULT_MAX_POOLSIZE);
    }

    public static PooledDataSource pooledReadOnlyDb(Properties properties, String configPrefix, int maxPoolSize) {
        String hostnameList = properties.getProperty(configPrefix + "read_only_cluster");

        if (hostnameList == null) {
            hostnameList = properties.getProperty(configPrefix + "hostname");
            if (hostnameList == null) {
                throw new IllegalArgumentException("Neither " + configPrefix+"read_only_cluster nor " + configPrefix + "hostname property available to configure data source");
            }
        }

        return pooled(
                hostnameList,
                Integer.parseInt(properties.getProperty(configPrefix + "port")),
                properties.getProperty(configPrefix + "username"),
                properties.getProperty(configPrefix + "password"),
                properties.getProperty(configPrefix + "database"),
                properties.getProperty(configPrefix + "driver"),
                maxPoolSize
        );
    }

    public static PooledDataSource pooledMasterDb(Config config) {
        return pooledMasterDb(config, DEFAULT_MAX_POOLSIZE);
    }

    public static PooledDataSource pooledMasterDb(Config config, int maxPoolSize) {
        return pooled(
                config.getString("hostname"),
                config.getInt("port"),
                config.getString("username"),
                config.getString("password"),
                config.getString("database"),
                config.getString("driver"),
                maxPoolSize
        );
    }


    public static PooledDataSource pooledReadOnlyDb(Config config) {
        return pooledReadOnlyDb(config, DEFAULT_MAX_POOLSIZE);
    }

    public static PooledDataSource pooledReadOnlyDb(Config config, int maxPoolSize) {
        return pooled(
                config.getString("read_only_cluster"),
                config.getInt("port"),
                config.getString("username"),
                config.getString("password"),
                config.getString("database"),
                config.getString("driver"),
                maxPoolSize
        );
    }

    private static PooledDataSource pooled(String hostname, int port, String username, String password, String database, String driver, int maxPoolsize) {
        ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(format("jdbc:mysql://%s:%d/%s?rewriteBatchedStatements=true",
                hostname,
                port,
                database));
        dataSource.setUser(username);
        dataSource.setPassword(password);
        dataSource.setIdleConnectionTestPeriod(60 * 5);
        dataSource.setMinPoolSize(3);
        dataSource.setInitialPoolSize(3);
        dataSource.setAcquireIncrement(1);
        dataSource.setMaxPoolSize(maxPoolsize);

        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return dataSource;
    }
}
