package com.timgroup.eventstore.mysql;

import com.codahale.metrics.MetricRegistry;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.PooledDataSource;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.prometheus.client.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import static java.lang.String.format;

public final class StacksConfiguredDataSource {

    private static final Logger LOG = LoggerFactory.getLogger(StacksConfiguredDataSource.class);

    private static final Gauge databaseConnections = Gauge.build("database_connections", "TG Eventstore Database connections")
            .labelNames("database", "state")
            .register();

    public static final int DEFAULT_MAX_POOLSIZE = 15;
    public static final int DEFAULT_SOCKET_TIMEOUT_MS = 15000;

    private static final SecretsManagerClient secretsManager = SecretsManagerClient.create();

    private StacksConfiguredDataSource() { /* prevent instantiation */ }

    public static PooledDataSource pooledMasterDb(Properties properties, String configPrefix) {
        return pooledMasterDb(properties, configPrefix, null);
    }

    /**
     * @deprecated Use corresponding API without metric registry
     */
    @Deprecated
    public static PooledDataSource pooledMasterDb(Properties properties, String configPrefix, @Nullable MetricRegistry metricRegistry) {
        return pooledMasterDb(properties, configPrefix, DEFAULT_MAX_POOLSIZE, metricRegistry);
    }

    public static PooledDataSource pooledMasterDb(Properties properties, String configPrefix, int maxPoolSize) {
        return pooledMasterDb(properties, configPrefix, maxPoolSize, null);
    }

    /**
     * @deprecated Use corresponding API without metric registry
     */
    @Deprecated
    public static PooledDataSource pooledMasterDb(Properties properties, String configPrefix, int maxPoolSize, @Nullable MetricRegistry metricRegistry) {
        return getPooledDataSource(properties, configPrefix, "hostname", maxPoolSize, DEFAULT_SOCKET_TIMEOUT_MS, metricRegistry);
    }

    public static PooledDataSource pooledReadOnlyDb(Properties properties, String configPrefix) {
        return pooledReadOnlyDb(properties, configPrefix, null);
    }

    /**
     * @deprecated Use corresponding API without metric registry
     */
    @Deprecated
    public static PooledDataSource pooledReadOnlyDb(Properties properties, String configPrefix, @Nullable MetricRegistry metricRegistry) {
        return pooledReadOnlyDb(properties, configPrefix, DEFAULT_MAX_POOLSIZE, metricRegistry);
    }

    public static PooledDataSource pooledReadOnlyDb(Properties properties, String configPrefix, int maxPoolSize) {
        return pooledReadOnlyDb(properties, configPrefix, maxPoolSize, null);
    }

    /**
     * @deprecated Use corresponding API without metric registry
     */
    @Deprecated
    public static PooledDataSource pooledReadOnlyDb(Properties properties, String configPrefix, int maxPoolSize, @Nullable MetricRegistry metricRegistry) {
        return pooledReadOnlyDb(properties, configPrefix, maxPoolSize, DEFAULT_SOCKET_TIMEOUT_MS, metricRegistry);
    }

    public static PooledDataSource pooledReadOnlyDb(Properties properties, String configPrefix, int maxPoolSize, int socketTimeoutMs) {
        return getPooledDataSource(properties, configPrefix, "read_only_cluster", maxPoolSize, socketTimeoutMs, null);
    }

    /**
     * @deprecated Use corresponding API without metric registry
     */
    @Deprecated
    public static PooledDataSource pooledReadOnlyDb(Properties properties, String configPrefix, int maxPoolSize, int socketTimeoutMs, @Nullable MetricRegistry metricRegistry) {
        return getPooledDataSource(properties, configPrefix, "read_only_cluster", maxPoolSize, socketTimeoutMs, metricRegistry);
    }

    private static PooledDataSource getPooledDataSource(Properties properties, String configPrefix, String hostProperty, int maxPoolSize, int socketTimeoutMs, @Nullable MetricRegistry metricRegistry) {
        String prefix = configPrefix;

        if (properties.getProperty(prefix + hostProperty) == null) {
            prefix = "db." + prefix + ".";
            if (properties.getProperty(prefix + hostProperty) == null) {
                throw new IllegalArgumentException("No " + configPrefix + hostProperty + " property available to configure data source");
            }
        }

        return pooled(
                properties.getProperty(prefix + hostProperty),
                Integer.parseInt(properties.getProperty(prefix + "port")),
                properties.getProperty(prefix + "username"),
                properties.getProperty(prefix + "password"),
                properties.getProperty(prefix + "secret_id"),
                properties.getProperty(prefix + "database"),
                properties.getProperty(prefix + "driver"),
                maxPoolSize,
                socketTimeoutMs,
                metricRegistry
        );
    }

    public static PooledDataSource pooledMasterDb(Config config) {
        return pooledMasterDb(config, DEFAULT_MAX_POOLSIZE, null);
    }

    /**
     * @deprecated Use corresponding API without metric registry
     */
    @Deprecated
    public static PooledDataSource pooledMasterDb(Config config, @Nullable MetricRegistry metricRegistry) {
        return pooledMasterDb(config, DEFAULT_MAX_POOLSIZE, metricRegistry);
    }

    public static PooledDataSource pooledMasterDb(Config config, int maxPoolSize) {
        return pooledMasterDb(config, maxPoolSize, null);
    }

    private static final Config DATA_SOURCE_FALLBACK = ConfigFactory.parseString("username=\nsecret_id=");

    /**
     * @deprecated Use corresponding API without metric registry
     */
    @Deprecated
    public static PooledDataSource pooledMasterDb(Config config, int maxPoolSize, @Nullable MetricRegistry metricRegistry) {
        Config defaultedConfig = config.withFallback(DATA_SOURCE_FALLBACK);
        return pooled(
                defaultedConfig.getString("hostname"),
                defaultedConfig.getInt("port"),
                defaultedConfig.getString("username"),
                defaultedConfig.getString("password"),
                defaultedConfig.getString("secret_id"),
                defaultedConfig.getString("database"),
                defaultedConfig.getString("driver"),
                maxPoolSize,
                DEFAULT_SOCKET_TIMEOUT_MS,
                metricRegistry
        );
    }

    public static PooledDataSource pooledReadOnlyDb(Config config) {
        return pooledReadOnlyDb(config, null);
    }

    /**
     * @deprecated Use corresponding API without metric registry
     */
    @Deprecated
    public static PooledDataSource pooledReadOnlyDb(Config config, @Nullable MetricRegistry metricRegistry) {
        return pooledReadOnlyDb(config, DEFAULT_MAX_POOLSIZE, metricRegistry);
    }

    public static PooledDataSource pooledReadOnlyDb(Config config, int maxPoolSize) {
        return pooledReadOnlyDb(config, maxPoolSize, null);
    }

    /**
     * @deprecated Use corresponding API without metric registry
     */
    @Deprecated
    public static PooledDataSource pooledReadOnlyDb(Config config, int maxPoolSize, @Nullable MetricRegistry metricRegistry) {
        Config defaultedConfig = config.withFallback(DATA_SOURCE_FALLBACK);
        return pooled(
                defaultedConfig.getString("read_only_cluster"),
                defaultedConfig.getInt("port"),
                defaultedConfig.getString("username"),
                defaultedConfig.getString("password"),
                defaultedConfig.getString("secret_id"),
                defaultedConfig.getString("database"),
                defaultedConfig.getString("driver"),
                maxPoolSize,
                DEFAULT_SOCKET_TIMEOUT_MS,
                metricRegistry
        );
    }

    private static PooledDataSource pooled(
            String hostname,
            int port,
            @Nullable String username,
            @Nullable String password,
            @Nullable String secretId,
            String database,
            String driver,
            int maxPoolsize,
            int socketTimeoutMs,
            @Nullable MetricRegistry metricRegistry) {
        ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(format("jdbc:mysql://%s:%d/%s?useSSL=false&rewriteBatchedStatements=true&secondsBeforeRetryMaster=1&queriesBeforeRetryMaster=1&connectTimeout=5000&socketTimeout=" + socketTimeoutMs,
                hostname,
                port,
                database));
        dataSource.setUser(username);
        if (password != null && !password.isEmpty()) {
            dataSource.setPassword(password);
        } else if (secretId != null && !secretId.isEmpty()) {
            GetSecretValueResponse response = secretsManager.getSecretValue(r -> r.secretId(secretId));
            Optional<CredentialsInSecret> jsonCredentials = CredentialsInSecret.extract(response.secretString());
            if (jsonCredentials.isPresent()) {
                dataSource.setPassword(jsonCredentials.get().password);

                // do not override directly-specified property
                if (username == null || username.isEmpty()) {
                    LOG.info("Read JSON database credentials from {}", response.arn());
                    dataSource.setUser(jsonCredentials.get().username);
                } else {
                    LOG.info("Read JSON database password for {} from {}", username, response.arn());
                }
            } else {
                dataSource.setPassword(response.secretString());
                LOG.info("Read raw database password for {} from {}", username, response.arn());
            }
        }
        dataSource.setIdleConnectionTestPeriod(60 * 5);
        dataSource.setMinPoolSize(Math.min(3, maxPoolsize));
        dataSource.setInitialPoolSize(3);
        dataSource.setAcquireIncrement(1);
        dataSource.setAcquireRetryAttempts(5);
        dataSource.setMaxPoolSize(maxPoolsize);
        dataSource.setMaxConnectionAge(60);
        dataSource.setPreferredTestQuery("SELECT 1");

        if (metricRegistry == null) {
            collectPrometheusMetrics(dataSource, database);
        } else {
            configureGraphiteMetrics(dataSource, database, metricRegistry);
        }

        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return dataSource;
    }

    private static void configureGraphiteMetrics(PooledDataSource dataSource, String databaseName, MetricRegistry metricRegistry) {
        sendCollectionState(metricRegistry, databaseName, "activeConnections", dataSource::getNumBusyConnectionsDefaultUser);
        sendCollectionState(metricRegistry, databaseName, "idleConnections", dataSource::getNumIdleConnectionsDefaultUser);
        sendCollectionState(metricRegistry, databaseName, "totalConnections", dataSource::getNumConnectionsDefaultUser);
        sendCollectionState(metricRegistry, databaseName, "unclosedOrphanedConnections", dataSource::getNumUnclosedOrphanedConnectionsDefaultUser);
    }

    private static void sendCollectionState(MetricRegistry metrics, String databaseName, String name, DataSourceMetric source) {
        metrics.gauge(String.format("database.%s.dataSource.%s.value", databaseName, name), () -> () -> {
            try {
                return source.get();
            } catch (SQLException e) {
                LOG.warn("Failed to fetch '" + name + "' metric from data source");
                return null;
            }
        });
    }

    private static void collectPrometheusMetrics(PooledDataSource dataSource, String databaseName) {
        collectConnectionState(databaseName, "activeConnections", dataSource::getNumBusyConnectionsDefaultUser);
        collectConnectionState(databaseName, "idleConnections", dataSource::getNumIdleConnectionsDefaultUser);
        collectConnectionState(databaseName, "totalConnections", dataSource::getNumConnectionsDefaultUser);
        collectConnectionState(databaseName, "unclosedOrphanedConnections", dataSource::getNumUnclosedOrphanedConnectionsDefaultUser);
    }

    private static void collectConnectionState(String databaseName, String connectionState, DataSourceMetric source) {
        databaseConnections.setChild(new Gauge.Child() {
            @Override
            public double get() {
                try {
                    return source.get();
                } catch (SQLException e) {
                    LOG.warn("Failed to fetch " + connectionState + " metric from database " + databaseName, e);
                    return -1;
                }
            }
        }, databaseName, connectionState);
    }

    @FunctionalInterface
    private interface DataSourceMetric {
        int get() throws SQLException;
    }
}
