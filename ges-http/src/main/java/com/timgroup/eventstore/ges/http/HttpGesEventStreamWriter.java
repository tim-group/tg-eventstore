package com.timgroup.eventstore.ges.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.StreamId;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

public class HttpGesEventStreamWriter implements EventStreamWriter {
    private final String host;

    public HttpGesEventStreamWriter(String host) {
        this.host = host;
    }

    @Override
    public void write(StreamId streamId, Collection<NewEvent> events) {
        try {
            CloseableHttpClient client = HttpClientBuilder.create().build();

            HttpPost writeRequest = new HttpPost("/streams/" + streamId.category() + "-" + streamId.id());

            //todo: metadata
            //todo: non-json body?
            List<HttpEvent> httpEvents = events.stream().map(e -> new HttpEvent(UUID.randomUUID().toString(), e.type(), new String(e.data(), UTF_8))).collect(toList());

            byte[] bytes = new ObjectMapper().writer().writeValueAsBytes(httpEvents);

            writeRequest.setEntity(new ByteArrayEntity(bytes, ContentType.create("application/vnd.eventstore.events+json")));

            client.execute(HttpHost.create(host), writeRequest, response -> {
                if (response.getStatusLine().getStatusCode() != 201) {
                    throw new RuntimeException("Write request failed: " + response.getStatusLine());
                }
                return null;
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(StreamId streamId, Collection<NewEvent> events, long expectedVersion) {

    }


    private static class HttpEvent {
        public final String eventId;
        public final String eventType;
        public final String data;

        private HttpEvent(String eventId, String eventType, String data) {
            this.eventId = eventId;
            this.eventType = eventType;
            this.data = data;
        }
    }

    public static class WriteResposne {

    }
}
