package com.timgroup.eventstore.mysql;

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

import java.util.Collection;

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
        String label = "EventStore (" + this.name + ")";
        return singletonList(new DatabaseConnectionComponent(label, label, connectionProvider::getConnection));
    }


    public static BasicMysqlEventSource pooledMasterDbEventSource(Config config, String tableName, String name) {
        ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(format("jdbc:mysql://%s:%d/%s?rewriteBatchedStatements=true",
                config.getString("hostname"),
                config.getInt("port"),
                config.getString("database")));
        dataSource.setUser(config.getString("username"));
        dataSource.setPassword(config.getString("password"));

        try {
            Class.forName(config.getString("driver"));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        new BasicMysqlEventStoreSetup(dataSource::getConnection, tableName).lazyCreate();

        return new BasicMysqlEventSource(dataSource::getConnection, tableName, DefaultBatchSize, name);
    }
}
