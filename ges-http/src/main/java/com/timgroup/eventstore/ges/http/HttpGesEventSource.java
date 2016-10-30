package com.timgroup.eventstore.ges.http;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.tucker.info.Component;

import java.util.Collection;

import static java.util.Collections.emptyList;

public class HttpGesEventSource implements EventSource {
    private final String host;

    public HttpGesEventSource(String host) {
        this.host = host;
    }

    @Override
    public EventReader readAll() {
        return null;
    }

    @Override
    public EventCategoryReader readCategory() {
        return null;
    }

    @Override
    public EventStreamReader readStream() {
        return new HttpGesEventStreamReader(host);
    }

    @Override
    public EventStreamWriter writeStream() {
        return new HttpGesEventStreamWriter(host);
    }

    @Override
    public PositionCodec positionCodec() {
        return null;
    }

    @Override
    public Collection<Component> monitoring() {
        return emptyList();
    }
}
