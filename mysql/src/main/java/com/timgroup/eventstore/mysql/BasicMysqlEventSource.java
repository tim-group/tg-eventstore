package com.timgroup.eventstore.mysql;

import com.codahale.metrics.MetricRegistry;
import com.mchange.v2.c3p0.PooledDataSource;
import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.tucker.info.Component;
import com.typesafe.config.Config;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;

import static java.util.Collections.singletonList;

public class BasicMysqlEventSource implements EventSource {
    private static final int DefaultBatchSize = 100000;

    private final ConnectionProvider connectionProvider;
    private final String databaseName;
    private final String tableName;
    private final int batchSize;
    private final String name;
    private final MetricRegistry metricRegistry;

    public BasicMysqlEventSource(ConnectionProvider connectionProvider, String tableName, int batchSize, String name, @Nullable MetricRegistry metricRegistry) {
        this.connectionProvider = connectionProvider;
        this.databaseName = databaseName(connectionProvider);
        this.tableName = tableName;
        this.batchSize = batchSize;
        this.name = name;
        this.metricRegistry = metricRegistry;
    }

    public BasicMysqlEventSource(ConnectionProvider connectionProvider, String tableName, int batchSize, MetricRegistry metricRegistry) {
        this(connectionProvider, tableName, batchSize, "EventStore", metricRegistry);
    }

    public BasicMysqlEventSource(ConnectionProvider connectionProvider, String tableName, MetricRegistry metricRegistry) {
        this(connectionProvider, tableName, DefaultBatchSize, metricRegistry);
    }

    @Nonnull
    @Override
    public EventReader readAll() {
        return new BasicMysqlEventReader(connectionProvider, databaseName, tableName, batchSize, metricRegistry);
    }

    @Nonnull
    @Override
    public EventCategoryReader readCategory() {
        return new BasicMysqlEventCategoryReader(connectionProvider, databaseName, tableName, batchSize, metricRegistry);
    }

    @Nonnull
    @Override
    public EventStreamReader readStream() {
        return new BasicMysqlEventStreamReader(connectionProvider, databaseName, tableName, batchSize, metricRegistry);
    }

    @Nonnull
    @Override
    public EventStreamWriter writeStream() {
        return new BasicMysqlEventStreamWriter(connectionProvider, databaseName, tableName, metricRegistry);
    }

    @Nonnull
    @Override
    public PositionCodec positionCodec() {
        return BasicMysqlEventStorePosition.CODEC;
    }

    @Nonnull
    @Override
    public Collection<Component> monitoring() {
        String id = "EventStore-" + this.name;
        String label = "EventStore (name=" + this.name + ", tableName=" + this.tableName +")";
        return singletonList(new EventStoreConnectionComponent(id, label, this));
    }

    @Override
    public String toString() {
        return "BasicMysqlEventSource{" +
                "tableName='" + tableName + '\'' +
                ", batchSize=" + batchSize +
                ", name='" + name + '\'' +
                '}';
    }

    /**
     * @deprecated  replaced by {@link #pooledMasterDbEventSource(Config config, String tableName, String name, MetricRegistry metricRegistry)}
     */
    @Deprecated
    public static PooledMysqlEventSource pooledMasterDbEventSource(Config config, String tableName, String name) {
        return pooledMasterDbEventSource(config, tableName, name, null);
    }

    public static PooledMysqlEventSource pooledMasterDbEventSource(Config config, String tableName, String name, @Nullable MetricRegistry metricRegistry) {
        return pooledMasterDbEventSource(config, tableName, name, DefaultBatchSize, metricRegistry);
    }

    /**
     * @deprecated  replaced by {@link #pooledMasterDbEventSource(Config config, String tableName, String name, int batchSize, MetricRegistry metricRegistry)}
     */
    @Deprecated
    public static PooledMysqlEventSource pooledMasterDbEventSource(Config config, String tableName, String name, int batchSize) {
        return pooledMasterDbEventSource(config, tableName, name, batchSize, null);
    }

    public static PooledMysqlEventSource pooledMasterDbEventSource(Config config, String tableName, String name, int batchSize, @Nullable MetricRegistry metricRegistry) {
        return pooledMasterDbEventSource(StacksConfiguredDataSource.pooledMasterDb(config, metricRegistry), tableName, name, batchSize, metricRegistry);
    }

    /**
     * @deprecated  replaced by {@link #pooledMasterDbEventSource(Properties properties, String configPrefix, String tableName, String name, MetricRegistry metricRegistry)}
     */
    @Deprecated
    public static PooledMysqlEventSource pooledMasterDbEventSource(Properties properties, String configPrefix, String tableName, String name) {
        return pooledMasterDbEventSource(properties, configPrefix, tableName, name, null);
    }

    public static PooledMysqlEventSource pooledMasterDbEventSource(Properties properties, String configPrefix, String tableName, String name, @Nullable MetricRegistry metricRegistry) {
        return pooledMasterDbEventSource(properties, configPrefix, tableName, name, DefaultBatchSize, metricRegistry);
    }

    /**
     * @deprecated  replaced by {@link #pooledMasterDbEventSource(Properties properties, String configPrefix, String tableName, String name, int batchSize, MetricRegistry metricRegistry)}
     */
    @Deprecated
    public static PooledMysqlEventSource pooledMasterDbEventSource(Properties properties, String configPrefix, String tableName, String name, int batchSize) {
        return pooledMasterDbEventSource(properties, configPrefix, tableName, name, batchSize, null);
    }

    public static PooledMysqlEventSource pooledMasterDbEventSource(Properties properties, String configPrefix, String tableName, String name, int batchSize, @Nullable MetricRegistry metricRegistry) {
        return pooledMasterDbEventSource(StacksConfiguredDataSource.pooledMasterDb(properties, configPrefix, metricRegistry), tableName, name, batchSize, metricRegistry);
    }

    /**
     * @deprecated  replaced by {@link #pooledMasterDbEventSource(PooledDataSource dataSource, String tableName, String name, MetricRegistry metricRegistry)}
     */
    @Deprecated
    public static PooledMysqlEventSource pooledMasterDbEventSource(PooledDataSource dataSource, String tableName, String name) {
        return pooledMasterDbEventSource(dataSource, tableName, name, null);
    }

    public static PooledMysqlEventSource pooledMasterDbEventSource(PooledDataSource dataSource, String tableName, String name, @Nullable MetricRegistry metricRegistry) {
        return pooledMasterDbEventSource(dataSource, tableName, name, DefaultBatchSize, metricRegistry);
    }

    /**
     * @deprecated  replaced by {@link #pooledReadOnlyDbEventSource(Config config, String tableName, String name, MetricRegistry metricRegistry)}
     */
    @Deprecated
    public static PooledMysqlEventSource pooledReadOnlyDbEventSource(Config config, String tableName, String name) {
        return pooledReadOnlyDbEventSource(config, tableName, name, null);
    }

    public static PooledMysqlEventSource pooledReadOnlyDbEventSource(Config config, String tableName, String name, @Nullable MetricRegistry metricRegistry) {
        return pooledReadOnlyDbEventSource(config, tableName, name, DefaultBatchSize, metricRegistry);
    }

    /**
     * @deprecated  replaced by {@link #pooledReadOnlyDbEventSource(Config config, String tableName, String name, int batchSize, MetricRegistry metricRegistry)}
     */
    @Deprecated
    public static PooledMysqlEventSource pooledReadOnlyDbEventSource(Config config, String tableName, String name, int batchSize) {
        return pooledReadOnlyDbEventSource(config, tableName, name, batchSize, null);
    }

    public static PooledMysqlEventSource pooledReadOnlyDbEventSource(Config config, String tableName, String name, int batchSize, @Nullable MetricRegistry metricRegistry) {
        return new PooledMysqlEventSource(StacksConfiguredDataSource.pooledReadOnlyDb(config, metricRegistry), tableName, batchSize, name, metricRegistry);
    }

    /**
     * @deprecated  replaced by {@link #pooledReadOnlyDbEventSource(Properties properties, String configPrefix, String tableName, String name, MetricRegistry metricRegistry)}
     */
    @Deprecated
    public static PooledMysqlEventSource pooledReadOnlyDbEventSource(Properties properties, String configPrefix, String tableName, String name) {
        return pooledReadOnlyDbEventSource(properties, configPrefix, tableName, name, null);
    }

    public static PooledMysqlEventSource pooledReadOnlyDbEventSource(Properties properties, String configPrefix, String tableName, String name, @Nullable MetricRegistry metricRegistry) {
        return pooledReadOnlyDbEventSource(properties, configPrefix, tableName, name, DefaultBatchSize, metricRegistry);
    }

    /**
     * @deprecated  replaced by {@link #pooledReadOnlyDbEventSource(Properties properties, String configPrefix, String tableName, String name, int batchSize, MetricRegistry metricRegistry)}
     */
    @Deprecated
    public static PooledMysqlEventSource pooledReadOnlyDbEventSource(Properties properties, String configPrefix, String tableName, String name, int batchSize) {
        return pooledReadOnlyDbEventSource(properties, configPrefix, tableName, name, batchSize, null);
    }

    public static PooledMysqlEventSource pooledReadOnlyDbEventSource(Properties properties, String configPrefix, String tableName, String name, int batchSize, @Nullable MetricRegistry metricRegistry) {
        return new PooledMysqlEventSource(StacksConfiguredDataSource.pooledReadOnlyDb(properties, configPrefix, metricRegistry), tableName, batchSize, name, metricRegistry);
    }

    /**
     * @deprecated  replaced by {@link #pooledReadOnlyDbEventSource(PooledDataSource dataSource, String tableName, String name, MetricRegistry metricRegistry)}
     */
    @Deprecated
    public static PooledMysqlEventSource pooledReadOnlyDbEventSource(PooledDataSource dataSource, String tableName, String name) {
        return pooledReadOnlyDbEventSource(dataSource, tableName, name, null);
    }

    public static PooledMysqlEventSource pooledReadOnlyDbEventSource(PooledDataSource dataSource, String tableName, String name, @Nullable MetricRegistry metricRegistry) {
        return new PooledMysqlEventSource(dataSource, tableName, DefaultBatchSize, name, metricRegistry);
    }

    /**
     * @deprecated  replaced by {@link #pooledReadOnlyDbEventSource(PooledDataSource dataSource, String tableName, String name, int batchSize, MetricRegistry metricRegistry)}
     */
    @Deprecated
    public static PooledMysqlEventSource pooledReadOnlyDbEventSource(PooledDataSource dataSource, String tableName, String name, int batchSize) {
        return pooledReadOnlyDbEventSource(dataSource, tableName, name, batchSize, null);
    }

    public static PooledMysqlEventSource pooledReadOnlyDbEventSource(PooledDataSource dataSource, String tableName, String name, int batchSize, @Nullable MetricRegistry metricRegistry) {
        return new PooledMysqlEventSource(dataSource, tableName, batchSize, name, metricRegistry);
    }

    /**
     * @deprecated  replaced by {@link #pooledMasterDbEventSource(PooledDataSource dataSource, String tableName, String name, int batchSize, MetricRegistry metricRegistry)}
     */
    @Deprecated
    public static PooledMysqlEventSource pooledMasterDbEventSource(PooledDataSource dataSource, String tableName, String name, int batchSize) {
        return pooledMasterDbEventSource(dataSource, tableName, name, batchSize, null);
    }

    public static PooledMysqlEventSource pooledMasterDbEventSource(PooledDataSource dataSource, String tableName, String name, int batchSize, @Nullable MetricRegistry metricRegistry) {
        try {
            new BasicMysqlEventStoreSetup(dataSource::getConnection, tableName).lazyCreate();
        } catch (Exception e) {
            LoggerFactory.getLogger(BasicMysqlEventSource.class).warn("Failed to ensure ES scheme is created", e);
        }

        return new PooledMysqlEventSource(dataSource, tableName, batchSize, name, metricRegistry);
    }


    public static final class PooledMysqlEventSource extends BasicMysqlEventSource implements AutoCloseable {
        private final PooledDataSource dataSource;

        /**
         * @deprecated  replaced by {@link #PooledMysqlEventSource(PooledDataSource dataSource, String tableName, int defaultBatchSize, String name, MetricRegistry metricRegistry)}
         */
        @Deprecated
        public PooledMysqlEventSource(PooledDataSource dataSource, String tableName, int defaultBatchSize, String name) {
            this(dataSource, tableName, defaultBatchSize, name, null);
        }

        public PooledMysqlEventSource(PooledDataSource dataSource, String tableName, int defaultBatchSize, String name, @Nullable MetricRegistry metricRegistry) {
            super(dataSource::getConnection, tableName, defaultBatchSize, name, metricRegistry);
            this.dataSource = dataSource;
        }

        @Override
        public void close() {
            try {
                dataSource.close();
            } catch (SQLException e) {
                LoggerFactory.getLogger(PooledMysqlEventSource.class).warn("Failed to close event source", e);
            }
        }
    }

    private static String databaseName(ConnectionProvider connectionProvider) {
        try (Connection connection = connectionProvider.getConnection()) {
            return connection.getCatalog();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
