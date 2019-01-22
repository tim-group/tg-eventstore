package com.timgroup.eventstore.ges.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.api.WrongExpectedVersionException;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

import static java.lang.Long.MIN_VALUE;
import static java.util.stream.Collectors.toList;

@ParametersAreNonnullByDefault
public class HttpGesEventStreamWriter implements EventStreamWriter {
    private final ObjectMapper mapper = new ObjectMapper();
    private final String host;

    public HttpGesEventStreamWriter(String host) {
        this.host = host;
    }

    @Override
    public void write(StreamId streamId, Collection<NewEvent> events) {
        write(streamId, events, OptionalLong.empty());
    }

    @Override
    public void write(StreamId streamId, Collection<NewEvent> events, long expectedVersion) {
        write(streamId, events, OptionalLong.of(expectedVersion));
    }


    @Override
    public String toString() {
        return "HttpGesEventStreamWriter{" +
                "mapper=" + mapper +
                ", host='" + host + '\'' +
                '}';
    }

    private void write(StreamId streamId, Collection<NewEvent> events, OptionalLong maybeExpectedVersion) {
        try {
            CloseableHttpClient client = HttpClientBuilder.create().build();

            HttpPost writeRequest = new HttpPost("/streams/" + streamId.category() + "-" + streamId.id());

            maybeExpectedVersion.ifPresent(expectedVersion -> {
                writeRequest.setHeader("ES-ExpectedVersion", Long.toString(expectedVersion));
            });

            List<HttpEvent> httpEvents = events.stream()
                    .map(e -> {
                        try {
                            return new HttpEvent(
                                    UUID.randomUUID().toString(),
                                    e.type(),
                                    readNonNullTree(e.data()),
                                    readNonNullTree(e.metadata())
                            );
                        } catch (IOException e1) {
                            throw new RuntimeException(e1);
                        }
                    }).collect(toList());

            byte[] bytes = mapper.writer().writeValueAsBytes(httpEvents);

            writeRequest.setEntity(new ByteArrayEntity(bytes, ContentType.create("application/vnd.eventstore.events+json")));

            client.execute(HttpHost.create(host), writeRequest, response -> {
                if (response.getStatusLine().getStatusCode() == 400 && response.getStatusLine().getReasonPhrase().equals("Wrong expected EventNumber")) {
                    //todo: remove need for MIN_VALUE
                    throw new WrongExpectedVersionException(MIN_VALUE, maybeExpectedVersion.getAsLong());
                } else if (response.getStatusLine().getStatusCode() != 201) {
                    throw new RuntimeException("Write request failed: " + response.getStatusLine());
                }
                return null;
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private JsonNode readNonNullTree(byte[] data) throws IOException {
        return Optional.ofNullable(mapper.readTree(data)).orElseThrow(() -> new IOException("blank json data"));
    }

    private static class HttpEvent {
        public final String eventId;
        public final String eventType;
        public final JsonNode data;
        public final JsonNode metadata;

        private HttpEvent(String eventId, String eventType, JsonNode data, JsonNode metadata) {
            this.eventId = eventId;
            this.eventType = eventType;
            this.data = data;
            this.metadata = metadata;
        }
    }
}
