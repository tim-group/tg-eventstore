package com.timgroup.eventstore.ges.http;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;

import java.util.stream.Stream;

public class HttpGesEventCategoryReader implements EventCategoryReader {
    private final EventStreamReader eventStreamReader;

    public HttpGesEventCategoryReader(EventStreamReader eventStreamReader) {
        this.eventStreamReader = eventStreamReader;
    }

    @Override
    public Stream<ResolvedEvent> readCategoryForwards(String category, Position positionExclusive) {
        return eventStreamReader.readStreamForwards(StreamId.streamId("$ce", category));
    }

    @Override
    public Position emptyCategoryPosition(String category) {
        return null;
    }
}
