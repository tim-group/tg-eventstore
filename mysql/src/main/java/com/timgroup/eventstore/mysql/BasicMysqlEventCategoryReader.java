package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.util.stream.Stream;

public class BasicMysqlEventCategoryReader implements EventCategoryReader {
    public BasicMysqlEventCategoryReader(ConnectionProvider connectionProvider) {

    }

    @Override
    public Stream<ResolvedEvent> readCategoryForwards(String category) {
        return null;
    }

    @Override
    public Stream<ResolvedEvent> readCategoryForwards(String category, Position position) {
        return null;
    }
}
