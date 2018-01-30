package com.timgroup.eventstore.mysql;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.PooledDataSource;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import static java.lang.String.format;

public final class StacksConfiguredDataSource {

    private static final Logger logger = LoggerFactory.getLogger(StacksConfiguredDataSource.class);

    public static final int DEFAULT_MAX_POOLSIZE = 15;

    private StacksConfiguredDataSource() { /* prevent instantiation */ }

    /**
     * @deprecated  replaced by {@link #pooledMasterDb(Properties properties, String configPrefix, MetricRegistry metricRegistry)}
     */
    @Deprecated
    public static PooledDataSource pooledMasterDb(Properties properties, String configPrefix) {
        return pooledMasterDb(properties, configPrefix, null);
    }

    public static PooledDataSource pooledMasterDb(Properties properties, String configPrefix, MetricRegistry metricRegistry) {
        return pooledMasterDb(properties, configPrefix, DEFAULT_MAX_POOLSIZE, metricRegistry);
    }

    /**
     * @deprecated  replaced by {@link #pooledMasterDb(Properties properties, String configPrefix, int maxPoolSize, MetricRegistry metricRegistry)}
     */
    @Deprecated
    public static PooledDataSource pooledMasterDb(Properties properties, String configPrefix, int maxPoolSize) {
        return pooledMasterDb(properties, configPrefix, maxPoolSize, null);
    }

    public static PooledDataSource pooledMasterDb(Properties properties, String configPrefix, int maxPoolSize, MetricRegistry metricRegistry) {
        return getPooledDataSource(properties, configPrefix, "hostname", maxPoolSize, metricRegistry);
    }

    /**
     * @deprecated  replaced by {@link #pooledReadOnlyDb(Properties properties, String configPrefix, MetricRegistry metricRegistry)}
     */
    @Deprecated
    public static PooledDataSource pooledReadOnlyDb(Properties properties, String configPrefix) {
        return pooledReadOnlyDb(properties, configPrefix, null);
    }

    public static PooledDataSource pooledReadOnlyDb(Properties properties, String configPrefix, MetricRegistry metricRegistry) {
        return pooledReadOnlyDb(properties, configPrefix, DEFAULT_MAX_POOLSIZE, metricRegistry);
    }

    /**
     * @deprecated  replaced by {@link #pooledReadOnlyDb(Properties properties, String configPrefix, int maxPoolSize, MetricRegistry metricRegistry)}
     */
    @Deprecated
    public static PooledDataSource pooledReadOnlyDb(Properties properties, String configPrefix, int maxPoolSize) {
        return pooledReadOnlyDb(properties, configPrefix, maxPoolSize, null);
    }

    public static PooledDataSource pooledReadOnlyDb(Properties properties, String configPrefix, int maxPoolSize, MetricRegistry metricRegistry) {
        return getPooledDataSource(properties, configPrefix, "read_only_cluster", maxPoolSize, metricRegistry);
    }

    private static PooledDataSource getPooledDataSource(Properties properties, String configPrefix, String host_propertyname, int maxPoolSize, MetricRegistry metricRegistry) {
        String prefix = configPrefix;

        if (properties.getProperty(prefix + host_propertyname) == null) {
            prefix = "db." + prefix + ".";
            if (properties.getProperty(prefix + host_propertyname) == null) {
                throw new IllegalArgumentException("No " + configPrefix + host_propertyname + " property available to configure data source");
            }
        }

        return pooled(
                properties.getProperty(prefix + host_propertyname),
                Integer.parseInt(properties.getProperty(prefix + "port")),
                properties.getProperty(prefix + "username"),
                properties.getProperty(prefix + "password"),
                properties.getProperty(prefix + "database"),
                properties.getProperty(prefix + "driver"),
                maxPoolSize,
                metricRegistry
        );
    }

    /**
     * @deprecated  replaced by {@link #pooledMasterDb(Config, MetricRegistry)}
     */
    @Deprecated
    public static PooledDataSource pooledMasterDb(Config config) {
        return pooledMasterDb(config, DEFAULT_MAX_POOLSIZE, null);
    }

    public static PooledDataSource pooledMasterDb(Config config, MetricRegistry metricRegistry) {
        return pooledMasterDb(config, DEFAULT_MAX_POOLSIZE, metricRegistry);
    }

    /**
     * @deprecated  replaced by {@link #pooledMasterDb(Config, int, MetricRegistry)}
     */
    @Deprecated
    public static PooledDataSource pooledMasterDb(Config config, int maxPoolSize) {
        return pooledMasterDb(config, maxPoolSize, null);
    }

    public static PooledDataSource pooledMasterDb(Config config, int maxPoolSize, MetricRegistry metricRegistry) {
        return pooled(
                config.getString("hostname"),
                config.getInt("port"),
                config.getString("username"),
                config.getString("password"),
                config.getString("database"),
                config.getString("driver"),
                maxPoolSize,
                metricRegistry
        );
    }

    /**
     * @deprecated  replaced by {@link #pooledReadOnlyDb(Config, MetricRegistry)}
     */
    @Deprecated
    public static PooledDataSource pooledReadOnlyDb(Config config) {
        return pooledReadOnlyDb(config, null);
    }

    public static PooledDataSource pooledReadOnlyDb(Config config, MetricRegistry metricRegistry) {
        return pooledReadOnlyDb(config, DEFAULT_MAX_POOLSIZE, metricRegistry);
    }

    /**
     * @deprecated  replaced by {@link #pooledReadOnlyDb(Config, int, MetricRegistry)}
     */
    @Deprecated
    public static PooledDataSource pooledReadOnlyDb(Config config, int maxPoolSize) {
        return pooledReadOnlyDb(config, maxPoolSize, null);
    }

    public static PooledDataSource pooledReadOnlyDb(Config config, int maxPoolSize, MetricRegistry metricRegistry) {
        return pooled(
                config.getString("read_only_cluster"),
                config.getInt("port"),
                config.getString("username"),
                config.getString("password"),
                config.getString("database"),
                config.getString("driver"),
                maxPoolSize,
                metricRegistry
        );
    }

    private static PooledDataSource pooled(
            String hostname,
            int port,
            String username,
            String password,
            String database,
            String driver,
            int maxPoolsize,
            MetricRegistry metricRegistry)
    {
        ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(format("jdbc:mysql://%s:%d/%s?rewriteBatchedStatements=true&connectTimeout=5000&socketTimeout=60000",
                hostname,
                port,
                database));
        dataSource.setUser(username);
        dataSource.setPassword(password);
        dataSource.setIdleConnectionTestPeriod(60 * 5);
        dataSource.setMinPoolSize(3);
        dataSource.setInitialPoolSize(3);
        dataSource.setAcquireIncrement(1);
        dataSource.setAcquireRetryAttempts(5);
        dataSource.setMaxPoolSize(maxPoolsize);
        dataSource.setMaxConnectionAge(30);
        dataSource.setPreferredTestQuery("SELECT 1");

        configureToSendMetrics(dataSource, database, metricRegistry);

        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return dataSource;
    }

    private static void configureToSendMetrics(PooledDataSource dataSource, String databaseName, MetricRegistry optionalMetricRegistry) {
        Optional.ofNullable(optionalMetricRegistry).ifPresent(metricRegistry -> {
            sendTo(metricRegistry, databaseName, "activeConnections", dataSource::getNumBusyConnectionsDefaultUser);
            sendTo(metricRegistry, databaseName, "idleConnections", dataSource::getNumIdleConnectionsDefaultUser);
            sendTo(metricRegistry, databaseName, "totalConnections", dataSource::getNumConnectionsDefaultUser);
            sendTo(metricRegistry, databaseName, "unclosedOrphanedConnections", dataSource::getNumUnclosedOrphanedConnectionsDefaultUser);
        });
    }

    private static void sendTo(MetricRegistry metrics, String databaseName, String name, DataSourceMetric source) {
        metrics.register(String.format("database.%s.dataSource.%s.value", databaseName,name), (Gauge<Integer>) () -> {
            try {
                return source.get();
            } catch (SQLException e) {
                logger.warn("Failed to fetch '" + name + "' metric from data source");
                return null;
            }
        });
    }

    @FunctionalInterface
    private interface DataSourceMetric {
        int get() throws SQLException;
    }
}
