package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.PositionCodec;

public class BasicMysqlEventSource implements EventSource {
    private final ConnectionProvider connectionProvider;
    private final String tableName;
    private final int batchSize;

    public BasicMysqlEventSource(ConnectionProvider connectionProvider, String tableName, int batchSize) {
        this.connectionProvider = connectionProvider;
        this.tableName = tableName;
        this.batchSize = batchSize;
    }

    public BasicMysqlEventSource(ConnectionProvider connectionProvider, String tableName) {
        this(connectionProvider, tableName, 100000);
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
}
