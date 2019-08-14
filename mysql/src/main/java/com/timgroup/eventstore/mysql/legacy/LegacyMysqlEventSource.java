package com.timgroup.eventstore.mysql.legacy;

import com.codahale.metrics.MetricRegistry;
import com.mchange.v2.c3p0.PooledDataSource;
import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.mysql.ConnectionProvider;
import com.timgroup.eventstore.mysql.StacksConfiguredDataSource;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.component.DatabaseConnectionComponent;
import com.typesafe.config.Config;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;

import static java.util.Collections.singletonList;

public class LegacyMysqlEventSource implements EventSource {
    private static final int DEFAULT_BATCH_SIZE = 100000;
    private static final StreamId DEFAULT_STREAM_ID = StreamId.streamId("all", "all");

    private final ConnectionProvider connectionProvider;
    private final String tableName;
    private final String name;

    private final LegacyMysqlEventReader eventReader;
    private final LegacyMysqlEventStreamWriter eventStreamWriter;

    public LegacyMysqlEventSource(ConnectionProvider connectionProvider, String tableName, StreamId pretendStreamId, int batchSize, String name, @Nullable MetricRegistry metricRegistry) {
        this.connectionProvider = connectionProvider;
        this.tableName = tableName;
        this.name = name;
        String database = databaseName(connectionProvider);
        this.eventReader = new LegacyMysqlEventReader(connectionProvider, database, tableName, pretendStreamId, batchSize, metricRegistry);
        this.eventStreamWriter = new LegacyMysqlEventStreamWriter(connectionProvider, database, tableName, pretendStreamId, metricRegistry);
    }

    public LegacyMysqlEventSource(ConnectionProvider connectionProvider, String tableName, int batchSize, MetricRegistry metricRegistry) {
        this(connectionProvider, tableName, DEFAULT_STREAM_ID, batchSize, "EventStore", metricRegistry);
    }

    public LegacyMysqlEventSource(ConnectionProvider connectionProvider, String tableName, MetricRegistry metricRegistry) {
        this(connectionProvider, tableName, DEFAULT_BATCH_SIZE, metricRegistry);
    }

    @Nonnull
    @Override
    public EventReader readAll() {
        return eventReader;
    }

    @Nonnull
    @Override
    public EventCategoryReader readCategory() {
        return eventReader;
    }

    @Nonnull
    @Override
    public EventStreamReader readStream() {
        return eventReader;
    }

    @Nonnull
    @Override
    public EventStreamWriter writeStream() {
        return eventStreamWriter;
    }

    @Nonnull
    @Override
    public Collection<Component> monitoring() {
        String id = "EventStore-" + this.name;
        String label = "EventStore (name=" + this.name + ", tableName=" + this.tableName +", legacy=true)";
        return singletonList(new DatabaseConnectionComponent(id, label, connectionProvider::getConnection));
    }

    @Override
    public String toString() {
        return "LegacyMysqlEventSource{" +
                "name='" + name + '\'' +
                ", eventReader=" + eventReader +
                ", eventStreamWriter=" + eventStreamWriter +
                '}';
    }

    /**
     * @deprecated  replaced by {@link #pooledMasterDbEventSource(Config config, String tableName, String name, MetricRegistry metricRegistry)}
     */
    public static LegacyPooledMysqlEventSource pooledMasterDbEventSource(Config config, String tableName, String name) {
        return pooledMasterDbEventSource(config, tableName, name, null);
    }

    public static LegacyPooledMysqlEventSource pooledMasterDbEventSource(Config config, String tableName, String name, @Nullable MetricRegistry metricRegistry) {
        return pooledMasterDbEventSource(config, tableName, DEFAULT_STREAM_ID, name, metricRegistry);
    }

    /**
     * @deprecated  replaced by {@link #pooledMasterDbEventSource(Config config, String tableName, String name, int batchSize, MetricRegistry metricRegistry)}
     */
    public static LegacyPooledMysqlEventSource pooledMasterDbEventSource(Config config, String tableName, String name, int batchSize) {
        return pooledMasterDbEventSource(config, tableName, name, batchSize, null);
    }

    public static LegacyPooledMysqlEventSource pooledMasterDbEventSource(Config config, String tableName, String name, int batchSize, @Nullable MetricRegistry metricRegistry) {
        return pooledMasterDbEventSource(config, tableName, DEFAULT_STREAM_ID, name, batchSize, metricRegistry);
    }

    /**
     * @deprecated  replaced by {@link #pooledMasterDbEventSource(Config config, String tableName, StreamId pretendStreamId, String name, MetricRegistry metricRegistry)}
     */
    public static LegacyPooledMysqlEventSource pooledMasterDbEventSource(Config config, String tableName, StreamId pretendStreamId, String name) {
        return pooledMasterDbEventSource(config, tableName, pretendStreamId, name, null);
    }

    public static LegacyPooledMysqlEventSource pooledMasterDbEventSource(Config config, String tableName, StreamId pretendStreamId, String name, @Nullable MetricRegistry metricRegistry) {
        return pooledMasterDbEventSource(config, tableName, pretendStreamId, name, DEFAULT_BATCH_SIZE, metricRegistry);
    }

    /**
     * @deprecated  replaced by {@link #pooledMasterDbEventSource(Config config, String tableName, StreamId pretendStreamId, String name, int batchSize, MetricRegistry metricRegistry)}
     */
    public static LegacyPooledMysqlEventSource pooledMasterDbEventSource(Config config, String tableName, StreamId pretendStreamId, String name, int batchSize) {
        return pooledMasterDbEventSource(config, tableName, pretendStreamId, name, batchSize, null);
    }

    public static LegacyPooledMysqlEventSource pooledMasterDbEventSource(Config config, String tableName, StreamId pretendStreamId, String name, int batchSize, @Nullable MetricRegistry metricRegistry) {
        return pooledEventSource(StacksConfiguredDataSource.pooledMasterDb(config, metricRegistry), tableName, pretendStreamId, name, batchSize, metricRegistry);
    }

    /**
     * @deprecated  replaced by {@link #pooledMasterDbEventSource(Properties properties, String configPrefix, String tableName, String name, MetricRegistry metricRegistry)}
     */
    public static LegacyPooledMysqlEventSource pooledMasterDbEventSource(Properties properties, String configPrefix, String tableName, String name) {
        return pooledMasterDbEventSource(properties, configPrefix, tableName, name, null);
    }


    public static LegacyPooledMysqlEventSource pooledMasterDbEventSource(Properties properties, String configPrefix, String tableName, String name, @Nullable MetricRegistry metricRegistry) {
        return pooledMasterDbEventSource(properties, configPrefix, tableName, DEFAULT_STREAM_ID, name, metricRegistry);
    }

    /**
     * @deprecated  replaced by {@link #pooledMasterDbEventSource(Properties properties, String configPrefix, String tableName, String name, int batchSize, MetricRegistry metricRegistry)}
     */
    @Deprecated
    public static LegacyPooledMysqlEventSource pooledMasterDbEventSource(Properties properties, String configPrefix, String tableName, String name, int batchSize) {
        return pooledMasterDbEventSource(properties, configPrefix, tableName, name, batchSize, null);
    }

    public static LegacyPooledMysqlEventSource pooledMasterDbEventSource(Properties properties, String configPrefix, String tableName, String name, int batchSize, @Nullable MetricRegistry metricRegistry) {
        return pooledMasterDbEventSource(properties, configPrefix, tableName, DEFAULT_STREAM_ID, name, batchSize, metricRegistry);
    }

    /**
     * @deprecated  replaced by {@link #pooledMasterDbEventSource(Properties properties, String configPrefix, String tableName, StreamId pretendStreamId, String name, MetricRegistry metricRegistry)}
     */
    @Deprecated
    public static LegacyPooledMysqlEventSource pooledMasterDbEventSource(Properties properties, String configPrefix, String tableName, StreamId pretendStreamId, String name) {
        return pooledMasterDbEventSource(properties, configPrefix, tableName, pretendStreamId, name, null);
    }

    public static LegacyPooledMysqlEventSource pooledMasterDbEventSource(Properties properties, String configPrefix, String tableName, StreamId pretendStreamId, String name, @Nullable MetricRegistry metricRegistry) {
        return pooledMasterDbEventSource(properties, configPrefix, tableName, pretendStreamId, name, DEFAULT_BATCH_SIZE, metricRegistry);
    }

    /**
     * @deprecated  replaced by {@link #pooledMasterDbEventSource(Properties properties, String configPrefix, String tableName, StreamId pretendStreamId, String name, int batchSize, MetricRegistry metricRegistry)}
     */
    @Deprecated
    public static LegacyPooledMysqlEventSource pooledMasterDbEventSource(Properties properties, String configPrefix, String tableName, StreamId pretendStreamId, String name, int batchSize) {
        return pooledMasterDbEventSource(properties, configPrefix, tableName, pretendStreamId, name, batchSize, null);
    }

    public static LegacyPooledMysqlEventSource pooledMasterDbEventSource(Properties properties, String configPrefix, String tableName, StreamId pretendStreamId, String name, int batchSize, @Nullable MetricRegistry metricRegistry) {
        return pooledEventSource(StacksConfiguredDataSource.pooledMasterDb(properties, configPrefix, metricRegistry), tableName, pretendStreamId, name, batchSize, metricRegistry);
    }

    /**
     * @deprecated  replaced by {@link #pooledReadOnlyDbEventSource(Properties properties, String configPrefix, String tableName, String name, MetricRegistry metricRegistry)}
     */
    @Deprecated
    public static LegacyPooledMysqlEventSource pooledReadOnlyDbEventSource(Properties properties, String configPrefix, String tableName, String name) {
        return pooledReadOnlyDbEventSource(properties, configPrefix, tableName, DEFAULT_STREAM_ID, name, null);
    }

    public static LegacyPooledMysqlEventSource pooledReadOnlyDbEventSource(Properties properties, String configPrefix, String tableName, String name, @Nullable MetricRegistry metricRegistry) {
        return pooledReadOnlyDbEventSource(properties, configPrefix, tableName, DEFAULT_STREAM_ID, name, metricRegistry);
    }

    /**
     * @deprecated  replaced by {@link #pooledReadOnlyDbEventSource(Properties properties, String configPrefix, String tableName, String name, int batchSize, MetricRegistry metricRegistry)}
     */
    @Deprecated
    public static LegacyPooledMysqlEventSource pooledReadOnlyDbEventSource(Properties properties, String configPrefix, String tableName, String name, int batchSize) {
        return pooledReadOnlyDbEventSource(properties, configPrefix, tableName, DEFAULT_STREAM_ID, name, batchSize, null);
    }

    public static LegacyPooledMysqlEventSource pooledReadOnlyDbEventSource(Properties properties, String configPrefix, String tableName, String name, int batchSize, @Nullable MetricRegistry metricRegistry) {
        return pooledReadOnlyDbEventSource(properties, configPrefix, tableName, DEFAULT_STREAM_ID, name, batchSize, metricRegistry);
    }

    /**
     * @deprecated  replaced by {@link #pooledReadOnlyDbEventSource(Properties properties, String configPrefix, String tableName, StreamId pretendStreamId, String name, MetricRegistry metricRegistry)}
     */
    @Deprecated
    public static LegacyPooledMysqlEventSource pooledReadOnlyDbEventSource(Properties properties, String configPrefix, String tableName, StreamId pretendStreamId, String name) {
        return pooledReadOnlyDbEventSource(properties, configPrefix, tableName, pretendStreamId, name, DEFAULT_BATCH_SIZE);
    }

    public static LegacyPooledMysqlEventSource pooledReadOnlyDbEventSource(Properties properties, String configPrefix, String tableName, StreamId pretendStreamId, String name, @Nullable MetricRegistry metricRegistry) {
        return pooledReadOnlyDbEventSource(properties, configPrefix, tableName, pretendStreamId, name, DEFAULT_BATCH_SIZE, metricRegistry);
    }

    /**
     * @deprecated  replaced by {@link #pooledReadOnlyDbEventSource(Properties properties, String configPrefix, String tableName, StreamId pretendStreamId, String name, int batchSize, MetricRegistry metricRegistry)}
     */
    @Deprecated
    public static LegacyPooledMysqlEventSource pooledReadOnlyDbEventSource(Properties properties, String configPrefix, String tableName, StreamId pretendStreamId, String name, int batchSize) {
        return pooledReadOnlyDbEventSource(properties, configPrefix, tableName, pretendStreamId, name, batchSize, null);
    }

    public static LegacyPooledMysqlEventSource pooledReadOnlyDbEventSource(Properties properties, String configPrefix, String tableName, StreamId pretendStreamId, String name, int batchSize, @Nullable MetricRegistry metricRegistry) {
        return pooledEventSource(StacksConfiguredDataSource.pooledReadOnlyDb(properties, configPrefix, metricRegistry), tableName, pretendStreamId, name, batchSize, metricRegistry);
    }


    public static LegacyPooledMysqlEventSource pooledEventSource(PooledDataSource dataSource, String tableName, StreamId pretendStreamId, String name, int batchSize, @Nullable MetricRegistry metricRegistry) {
        try {
            new LegacyMysqlEventStoreSetup(dataSource::getConnection, tableName).lazyCreate();
        } catch (Exception e) {
            LoggerFactory.getLogger(LegacyMysqlEventSource.class).warn("Failed to ensure ES scheme is created", e);
        }

        return new LegacyPooledMysqlEventSource(dataSource, tableName, pretendStreamId, batchSize, name, metricRegistry);
    }

    public static final class LegacyPooledMysqlEventSource extends LegacyMysqlEventSource implements AutoCloseable {
        private final PooledDataSource dataSource;

        public LegacyPooledMysqlEventSource(PooledDataSource dataSource, String tableName, StreamId pretendStreamId, int batchSize, String name, @Nullable MetricRegistry metricRegistry) {
            super(dataSource::getConnection, tableName, pretendStreamId, batchSize, name, metricRegistry);
            this.dataSource = dataSource;
        }

        @Override
        public void close() {
            try {
                dataSource.close();
            } catch (SQLException e) {
                LoggerFactory.getLogger(LegacyPooledMysqlEventSource.class).warn("Failed to close event source", e);
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
