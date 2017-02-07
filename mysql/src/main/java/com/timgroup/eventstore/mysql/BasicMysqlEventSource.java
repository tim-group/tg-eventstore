package com.timgroup.eventstore.mysql;

import java.util.Collection;
import java.util.Optional;
import java.util.Properties;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.component.DatabaseConnectionComponent;
import com.typesafe.config.Config;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;
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
        return new BasicMysqlPositionCodec();
    }

    @Override
    public Collection<Component> monitoring() {
        String id = "EventStore-" + this.name;
        String label = "EventStore (" + this.name + ")";
        Component databaseConnection = new DatabaseConnectionComponent(id, label, connectionProvider::getConnection);
        if (databaseConnectionInfo().isPresent()) {
            String prefix = databaseConnectionInfo().get() + ": ";
            databaseConnection = databaseConnection.mapValue(v -> prefix + v);
        }
        return singletonList(databaseConnection);
    }

    protected Optional<String> databaseConnectionInfo() {
        return Optional.empty();
    }

    public static PooledMysqlEventSource pooledMasterDbEventSource(Config config, String tableName, String name) {
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
            new BasicMysqlEventStoreSetup(dataSource::getConnection, tableName).lazyCreate();
        } catch (Exception e) {
            LoggerFactory.getLogger(BasicMysqlEventSource.class).warn("Failed to ensure ES scheme is created", e);
        }

        return new PooledMysqlEventSource(dataSource, tableName, DefaultBatchSize, name);
    }

    public static PooledMysqlEventSource pooledMasterDbEventSource(Properties properties, String configPrefix, String tableName, String name) {
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
            new BasicMysqlEventStoreSetup(dataSource::getConnection, tableName).lazyCreate();
        } catch (Exception e) {
            LoggerFactory.getLogger(BasicMysqlEventSource.class).warn("Failed to ensure ES scheme is created", e);
        }

        return new PooledMysqlEventSource(dataSource, tableName, DefaultBatchSize, name);
    }

    public static final class PooledMysqlEventSource extends BasicMysqlEventSource implements AutoCloseable {
        private final ComboPooledDataSource dataSource;
        private final String connectionInfo;

        public PooledMysqlEventSource(ComboPooledDataSource dataSource, String tableName, int defaultBatchSize, String name) {
            super(dataSource::getConnection, tableName, defaultBatchSize, name);
            this.connectionInfo = dataSource.getUser() == null ? dataSource.getJdbcUrl() : dataSource.getUser() + " @ " + dataSource.getJdbcUrl();
            this.dataSource = dataSource;
        }

        @Override
        public void close() {
            dataSource.close();
        }

        @Override
        protected Optional<String> databaseConnectionInfo() {
            return Optional.of(connectionInfo);
        }
    }
}
