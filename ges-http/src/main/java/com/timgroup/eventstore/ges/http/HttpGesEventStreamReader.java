package com.timgroup.eventstore.ges.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.NoSuchStreamException;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static java.nio.charset.StandardCharsets.UTF_8;

public class HttpGesEventStreamReader implements EventStreamReader {
    private final String host;

    public HttpGesEventStreamReader(String host) {
        this.host = host;
    }

    @Override
    public Stream<ResolvedEvent> readStreamForwards(StreamId streamId, long eventNumber) {
        try {
            CloseableHttpClient client = HttpClientBuilder.create().build();

            HttpGet readRequest = new HttpGet("/streams/" + streamId.category() + "-" + streamId.id() + "?embed=body");
            readRequest.setHeader("Accept", "application/json");

            //todo: pagination

            return client.execute(HttpHost.create(host), readRequest, response -> {
                if (response.getStatusLine().getStatusCode() == 404) {
                    throw new NoSuchStreamException(streamId);
                } else if (response.getStatusLine().getStatusCode() != 200) {
                    throw new RuntimeException("Read request failed: " + response.getStatusLine());
                }

                JsonNode jsonNode = new ObjectMapper().readTree(response.getEntity().getContent());;

                GesHttpResponse r = new ObjectMapper().registerModule(new JavaTimeModule()).configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
                        .readerFor(GesHttpResponse.class).readValue(jsonNode);

                return r.entries.stream()
                        .map(e -> new ResolvedEvent(new GesHttpPosition(), eventRecord(e.updated, e.streamId(), e.eventNumber, e.eventType, e.data.getBytes(UTF_8), e.metaData.getBytes(UTF_8))))
                        .sorted((e1, e2) -> Long.compare(e1.eventRecord().eventNumber(), e2.eventRecord().eventNumber()));
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class GesHttpPosition implements Position {

    }

    private static class GesHttpResponse {
        public final List<GesHttpReadEvent> entries;

        private GesHttpResponse() {
            entries = null;
        }
    }

    private static class GesHttpReadEvent {
        public final String eventType;
        public final long eventNumber;
        public final String streamId;
        public final Instant updated;
        public final String data;
        public final String metaData;


        private StreamId streamId() {
            int categorySeparatorPosition = streamId.indexOf('-');
            if (categorySeparatorPosition == -1) {
                throw new RuntimeException("StreamId " + streamId + " does not have a category");
            }

            return StreamId.streamId(streamId.substring(0, categorySeparatorPosition), streamId.substring(categorySeparatorPosition + 1));
        }

        private GesHttpReadEvent() {
            eventNumber = 0L;
            eventType = null;
            streamId = null;
            updated = null;
            data = null;
            metaData = null;
        }
    }
}
