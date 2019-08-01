package com.timgroup.eventstore.ges.http;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.tucker.info.Component;

import javax.annotation.Nonnull;
import java.util.Collection;

import static java.util.Collections.emptyList;

public class HttpGesEventSource implements EventSource {
    private final String host;

    public HttpGesEventSource(String host) {
        this.host = host;
    }

    @Nonnull
    @Override
    public EventReader readAll() {
        return new HttpGesEventReader(host);
    }

    @Nonnull
    @Override
    public EventCategoryReader readCategory() {
        return new HttpGesEventCategoryReader(readStream());
    }

    @Nonnull
    @Override
    public EventStreamReader readStream() {
        return new HttpGesEventStreamReader(host);
    }

    @Nonnull
    @Override
    public EventStreamWriter writeStream() {
        return new HttpGesEventStreamWriter(host);
    }

    @Nonnull
    @Override
    @Deprecated
    public PositionCodec positionCodec() {
        return HttpGesEventStreamReader.GesHttpPosition.CODEC;
    }

    @Nonnull
    @Override
    public Collection<Component> monitoring() {
        return emptyList();
    }

    @Override
    public String toString() {
        return "HttpGesEventSource{" +
                "host='" + host + '\'' +
                '}';
    }
}
