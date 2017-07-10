package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.*;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.component.DatabaseConnectionComponent;
import com.typesafe.config.Config;
import org.slf4j.LoggerFactory;

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

    private static PooledMysqlEventSource pooledMasterDbEventSource(StacksConfiguredDataSource stacksConfiguredDataSource, String tableName, String name, int batchSize) {
        try {
            new BasicMysqlEventStoreSetup(stacksConfiguredDataSource.dataSource::getConnection, tableName).lazyCreate();
        } catch (Exception e) {
            LoggerFactory.getLogger(BasicMysqlEventSource.class).warn("Failed to ensure ES scheme is created", e);
        }

        return new PooledMysqlEventSource(stacksConfiguredDataSource, tableName, batchSize, name);
    }

    public static final class PooledMysqlEventSource extends BasicMysqlEventSource implements AutoCloseable {
        private final StacksConfiguredDataSource dataSource;

        public PooledMysqlEventSource(StacksConfiguredDataSource stacksConfiguredDataSource, String tableName, int defaultBatchSize, String name) {
            super(stacksConfiguredDataSource.dataSource::getConnection, tableName, defaultBatchSize, name);
            this.dataSource = stacksConfiguredDataSource;
        }

        @Override
        public void close() {
            dataSource.close();
        }
    }
}
