package com.timgroup.eventstore.mysql.legacy;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.mysql.ConnectionProvider;
import com.timgroup.eventstore.mysql.StacksConfiguredDataSource;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.component.DatabaseConnectionComponent;
import com.typesafe.config.Config;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Properties;

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

    public static LegacyPooledMysqlEventSource pooledMasterDbEventSource(Config config, String tableName, String name, int batchSize) {
        return pooledMasterDbEventSource(config, tableName, DEFAULT_STREAM_ID, name, batchSize);
    }

    public static LegacyPooledMysqlEventSource pooledMasterDbEventSource(Config config, String tableName, StreamId pretendStreamId, String name) {
        return pooledMasterDbEventSource(config, tableName, pretendStreamId, name, DEFAULT_BATCH_SIZE);
    }

    public static LegacyPooledMysqlEventSource pooledMasterDbEventSource(Config config, String tableName, StreamId pretendStreamId, String name, int batchSize) {
        return pooledMasterDbEventSource(StacksConfiguredDataSource.pooled(config), tableName, pretendStreamId, name, batchSize);
    }



    public static LegacyPooledMysqlEventSource pooledMasterDbEventSource(Properties properties, String configPrefix, String tableName, String name) {
        return pooledMasterDbEventSource(properties, configPrefix, tableName, DEFAULT_STREAM_ID, name);
    }

    public static LegacyPooledMysqlEventSource pooledMasterDbEventSource(Properties properties, String configPrefix, String tableName, String name, int batchSize) {
        return pooledMasterDbEventSource(properties, configPrefix, tableName, DEFAULT_STREAM_ID, name, batchSize);
    }

    public static LegacyPooledMysqlEventSource pooledMasterDbEventSource(Properties properties, String configPrefix, String tableName, StreamId pretendStreamId, String name) {
        return pooledMasterDbEventSource(properties, configPrefix, tableName, pretendStreamId, name, DEFAULT_BATCH_SIZE);
    }

    public static LegacyPooledMysqlEventSource pooledMasterDbEventSource(Properties properties, String configPrefix, String tableName, StreamId pretendStreamId, String name, int batchSize) {
        return pooledMasterDbEventSource(StacksConfiguredDataSource.pooled(properties, configPrefix), tableName, pretendStreamId, name, batchSize);
    }


    private static LegacyPooledMysqlEventSource pooledMasterDbEventSource(ComboPooledDataSource dataSource, String tableName, StreamId pretendStreamId, String name, int batchSize) {
        try {
            new LegacyMysqlEventStoreSetup(dataSource::getConnection, tableName).lazyCreate();
        } catch (Exception e) {
            LoggerFactory.getLogger(LegacyMysqlEventSource.class).warn("Failed to ensure ES scheme is created", e);
        }

        return new LegacyPooledMysqlEventSource(dataSource, tableName, pretendStreamId, batchSize, name);
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
