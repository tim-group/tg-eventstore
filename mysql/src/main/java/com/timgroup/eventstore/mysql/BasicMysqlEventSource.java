package com.timgroup.eventstore.mysql;

import com.mchange.v2.c3p0.PooledDataSource;
import com.timgroup.eventstore.api.*;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.component.DatabaseConnectionComponent;
import com.typesafe.config.Config;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;

import static java.util.Collections.singletonList;

public class BasicMysqlEventSource implements EventSource {
    private static final int DefaultBatchSize = 100000;

    private final ConnectionProvider connectionProvider;
    private final String tableName;
    private final int batchSize;
    private final String name;

    public BasicMysqlEventSource(ConnectionProvider connectionProvider, String tableName, int batchSize, String name) {
        this.connectionProvider = connectionProvider;
        this.tableName = tableName;
        this.batchSize = batchSize;
        this.name = name;
    }

    public BasicMysqlEventSource(ConnectionProvider connectionProvider, String tableName, int batchSize) {
        this(connectionProvider, tableName, batchSize, "EventStore");
    }

    public BasicMysqlEventSource(ConnectionProvider connectionProvider, String tableName) {
        this(connectionProvider, tableName, DefaultBatchSize);
    }

    @Override
    public EventReader readAll() {
        return new BasicMysqlEventReader(connectionProvider, tableName, batchSize);
    }

    @Override
    public EventCategoryReader readCategory() {
        return new BasicMysqlEventCategoryReader(connectionProvider, tableName, batchSize);
    }

    @Override
    public EventStreamReader readStream() {
        return new BasicMysqlEventStreamReader(connectionProvider, tableName, batchSize);
    }

    @Override
    public EventStreamWriter writeStream() {
        return new BasicMysqlEventStreamWriter(connectionProvider, tableName);
    }

    @Override
    public PositionCodec positionCodec() {
        return BasicMysqlEventStorePosition.CODEC;
    }

    @Override
    public Collection<Component> monitoring() {
        String id = "EventStore-" + this.name;
        String label = "EventStore (" + this.name + ")";
        return singletonList(new DatabaseConnectionComponent(id, label, connectionProvider::getConnection));
    }

    public static PooledMysqlEventSource pooledMasterDbEventSource(Config config, String tableName, String name) {
        return pooledMasterDbEventSource(config, tableName, name, DefaultBatchSize);
    }

    public static PooledMysqlEventSource pooledMasterDbEventSource(Config config, String tableName, String name, int batchSize) {
        return pooledMasterDbEventSource(StacksConfiguredDataSource.pooledMasterDb(config), tableName, name, batchSize);
    }

    public static PooledMysqlEventSource pooledMasterDbEventSource(Properties properties, String configPrefix, String tableName, String name) {
        return pooledMasterDbEventSource(properties, configPrefix, tableName, name, DefaultBatchSize);
    }

    public static PooledMysqlEventSource pooledMasterDbEventSource(Properties properties, String configPrefix, String tableName, String name, int batchSize) {
        return pooledMasterDbEventSource(StacksConfiguredDataSource.pooledMasterDb(properties, configPrefix), tableName, name, batchSize);
    }

    public static PooledMysqlEventSource pooledReadOnlyDbEventSource(Config config, String tableName, String name) {
        return pooledReadOnlyDbEventSource(config, tableName, name, DefaultBatchSize);
    }

    public static PooledMysqlEventSource pooledReadOnlyDbEventSource(Config config, String tableName, String name, int batchSize) {
        return new PooledMysqlEventSource(StacksConfiguredDataSource.pooledReadOnlyDb(config), tableName, batchSize, name);
    }

    public static PooledMysqlEventSource pooledReadOnlyDbEventSource(Properties properties, String configPrefix, String tableName, String name) {
        return pooledReadOnlyDbEventSource(properties, configPrefix, tableName, name, DefaultBatchSize);
    }

    public static PooledMysqlEventSource pooledReadOnlyDbEventSource(Properties properties, String configPrefix, String tableName, String name, int batchSize) {
        return new PooledMysqlEventSource(StacksConfiguredDataSource.pooledReadOnlyDb(properties, configPrefix), tableName, batchSize, name);
    }

    public static PooledMysqlEventSource pooledMasterDbEventSource(PooledDataSource dataSource, String tableName, String name, int batchSize) {
        try {
            new BasicMysqlEventStoreSetup(dataSource::getConnection, tableName).lazyCreate();
        } catch (Exception e) {
            LoggerFactory.getLogger(BasicMysqlEventSource.class).warn("Failed to ensure ES scheme is created", e);
        }

        return new PooledMysqlEventSource(dataSource, tableName, batchSize, name);
    }

    public static final class PooledMysqlEventSource extends BasicMysqlEventSource implements AutoCloseable {
        private final PooledDataSource dataSource;

        public PooledMysqlEventSource(PooledDataSource dataSource, String tableName, int defaultBatchSize, String name) {
            super(dataSource::getConnection, tableName, defaultBatchSize, name);
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
}
