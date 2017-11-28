package com.timgroup.eventstore.ges.http;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.ges.http.HttpGesEventStreamReader.GesHttpPosition;

import java.util.stream.Stream;

import static com.timgroup.eventstore.api.StreamId.streamId;

public class HttpGesEventCategoryReader implements EventCategoryReader {
    private final EventStreamReader eventStreamReader;

    public HttpGesEventCategoryReader(EventStreamReader eventStreamReader) {
        this.eventStreamReader = eventStreamReader;
    }

    @Override
    public Stream<ResolvedEvent> readCategoryForwards(String category, Position positionExclusive) {
        return eventStreamReader.readStreamForwards(streamId("$ce", category), ((GesHttpPosition) positionExclusive).value);
    }

    @Override
    public Position emptyCategoryPosition(String category) {
        return new GesHttpPosition(-1L);
    }

    @Override
    public String toString() {
        return "HttpGesEventCategoryReader{" +
                "eventStreamReader=" + eventStreamReader +
                '}';
    }
}
