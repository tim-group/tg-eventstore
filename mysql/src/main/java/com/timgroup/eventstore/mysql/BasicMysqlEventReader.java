package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.stream.Stream;

public class BasicMysqlEventReader implements EventReader {
    public BasicMysqlEventReader(ConnectionProvider connectionProvider, String tableName) {

    }

    @Override
    public Stream<ResolvedEvent> readAllForwards() {
        return null;
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        return null;
    }
}
