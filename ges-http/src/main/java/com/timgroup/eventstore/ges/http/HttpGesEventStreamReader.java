package com.timgroup.eventstore.ges.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.timgroup.eventstore.api.EventStreamReader;
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

            HttpGet readRequest = new HttpGet("/streams/" + streamId.category() + "-" + streamId.id() + "?embed=tryharder");
            readRequest.setHeader("Accept", "application/json");

            //todo: pagination
            //todo: metadata
            //todo: streamId

            client.execute(HttpHost.create(host), readRequest, response -> {
                if (response.getStatusLine().getStatusCode() != 200) {
                    throw new RuntimeException("Write request failed: " + response.getStatusLine());
                }


                JsonNode jsonNode = new ObjectMapper().readTree(response.getEntity().getContent());;

                System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(jsonNode));

                GesHttpResponse r = new ObjectMapper().configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
                        .readerFor(GesHttpResponse.class).readValue(jsonNode);

//                new ObjectMapper().readTree(response.getEntity().getContent());

                return r.entries.stream()
                        .map(e -> new ResolvedEvent(null, eventRecord(e.updated, null, e.eventNumber, e.eventType, e.data.getBytes(UTF_8), new byte[0])))
                        .sorted((e1, e2) -> Long.compare(e1.eventRecord().eventNumber(), e2.eventRecord().eventNumber()));
            });
            return Stream.empty();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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


        private GesHttpReadEvent() {
            eventNumber = 0L;
            eventType = null;
            streamId = null;
            updated = null;
            data = null;
        }
    }
}
