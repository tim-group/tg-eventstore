package com.timgroup.eventstore.mysql.legacy;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.timgroup.eventstore.api.*;
import com.timgroup.eventstore.mysql.ConnectionProvider;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.component.DatabaseConnectionComponent;
import com.typesafe.config.Config;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Properties;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

public class LegacyMysqlEventSource implements EventSource {
    private static final int DEFAULT_BATCH_SIZE = 100000;
    private static final StreamId DEFAULT_STREAM_ID = StreamId.streamId("all", "all");

    private final ConnectionProvider connectionProvider;
    private final String name;

    private final LegacyMysqlEventReader eventReader;
    private final LegacyMysqlEventStreamWriter eventStreamWriter;
    private final LegacyMysqlEventPosition.LegacyPositionCodec positionCodec;

    public LegacyMysqlEventSource(ConnectionProvider connectionProvider, String tableName, StreamId pretendStreamId, int batchSize, String name) {
        this.connectionProvider = connectionProvider;
        this.name = name;
        this.eventReader = new LegacyMysqlEventReader(connectionProvider, tableName, pretendStreamId, batchSize);
        this.eventStreamWriter = new LegacyMysqlEventStreamWriter(connectionProvider, tableName, pretendStreamId);
        this.positionCodec = new LegacyMysqlEventPosition.LegacyPositionCodec();
    }

    public LegacyMysqlEventSource(ConnectionProvider connectionProvider, String tableName, int batchSize) {
        this(connectionProvider, tableName, DEFAULT_STREAM_ID, batchSize, "EventStore");
    }

    public LegacyMysqlEventSource(ConnectionProvider connectionProvider, String tableName) {
        this(connectionProvider, tableName, DEFAULT_BATCH_SIZE);
    }

    @Override
    public EventReader readAll() {
        return eventReader;
    }

    @Override
    public EventCategoryReader readCategory() {
        return eventReader;
    }

    @Override
    public EventStreamReader readStream() {
        return eventReader;
    }

    @Override
    public EventStreamWriter writeStream() {
        return eventStreamWriter;
    }

    @Override
    public PositionCodec positionCodec() {
        return positionCodec;
    }

    @Override
    public Collection<Component> monitoring() {
        String id = "EventStore-" + this.name;
        String label = "EventStore (" + this.name + ")";
        return singletonList(new DatabaseConnectionComponent(id, label, connectionProvider::getConnection));
    }

    public static LegacyPooledMysqlEventSource pooledMasterDbEventSource(Config config, String tableName, String name) {
        return pooledMasterDbEventSource(config, tableName, DEFAULT_STREAM_ID, name);
    }

    public static LegacyPooledMysqlEventSource pooledMasterDbEventSource(Config config, String tableName, StreamId pretendStreamId, String name) {
        ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(format("jdbc:mysql://%s:%d/%s?rewriteBatchedStatements=true",
                config.getString("hostname"),
                config.getInt("port"),
                config.getString("database")));
        dataSource.setUser(config.getString("username"));
        dataSource.setPassword(config.getString("password"));
        dataSource.setIdleConnectionTestPeriod(60 * 5);

        try {
            Class.forName(config.getString("driver"));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        try {
            new LegacyMysqlEventStoreSetup(dataSource::getConnection, tableName).lazyCreate();
        } catch (Exception e) {
            LoggerFactory.getLogger(LegacyMysqlEventSource.class).warn("Failed to ensure ES scheme is created", e);
        }

        return new LegacyPooledMysqlEventSource(dataSource, tableName, pretendStreamId, DEFAULT_BATCH_SIZE, name);
    }

    public static LegacyPooledMysqlEventSource pooledMasterDbEventSource(Properties properties, String configPrefix, String tableName, String name) {
        return pooledMasterDbEventSource(properties, configPrefix, tableName, DEFAULT_STREAM_ID, name);
    }

    public static LegacyPooledMysqlEventSource pooledMasterDbEventSource(Properties properties, String configPrefix, String tableName, StreamId pretendStreamId, String name) {
        ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(format("jdbc:mysql://%s:%d/%s?rewriteBatchedStatements=true",
                properties.getProperty(configPrefix + "hostname"),
                Integer.parseInt(properties.getProperty(configPrefix + "port")),
                properties.get(configPrefix + "database")));
        dataSource.setUser(properties.getProperty(configPrefix + "username"));
        dataSource.setPassword(properties.getProperty(configPrefix + "password"));

        try {
            Class.forName(properties.getProperty(configPrefix + "driver"));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        try {
            new LegacyMysqlEventStoreSetup(dataSource::getConnection, tableName).lazyCreate();
        } catch (Exception e) {
            LoggerFactory.getLogger(LegacyMysqlEventSource.class).warn("Failed to ensure ES scheme is created", e);
        }

        return new LegacyPooledMysqlEventSource(dataSource, tableName, pretendStreamId, DEFAULT_BATCH_SIZE, name);
    }

    public static final class LegacyPooledMysqlEventSource extends LegacyMysqlEventSource implements AutoCloseable {
        private final ComboPooledDataSource dataSource;

        public LegacyPooledMysqlEventSource(ComboPooledDataSource dataSource, String tableName, StreamId pretendStreamId, int batchSize, String name) {
            super(dataSource::getConnection, tableName, pretendStreamId, batchSize, name);
            this.dataSource = dataSource;
        }

        @Override
        public void close() {
            dataSource.close();
        }
    }

}
