package com.timgroup.eventstore.ges.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import org.apache.http.HttpHost;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.util.stream.Stream;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static org.apache.http.auth.AuthScope.ANY;

@ParametersAreNonnullByDefault
public class HttpGesEventReader implements EventReader {
    private final String host;

    public HttpGesEventReader(String host) {
        this.host = host;
    }

    @Nonnull
    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        try {
            BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(ANY, new UsernamePasswordCredentials("admin", "changeit"));
            CloseableHttpClient client = HttpClientBuilder.create().setDefaultCredentialsProvider(credentialsProvider).build();

            HttpGet readRequest = new HttpGet("/streams/$all?embed=body");
            readRequest.setHeader("Accept", "application/json");

            //todo: work out if we need to start with the head page and walk backwards
            //todo: pagination

            return client.execute(HttpHost.create(host), readRequest, response -> {
                if (response.getStatusLine().getStatusCode() != 200) {
                    throw new RuntimeException("Read request failed: " + response.getStatusLine());
                }

                JsonNode jsonNode = new ObjectMapper().readTree(response.getEntity().getContent());;

                System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(jsonNode));

                HttpGesEventStreamReader.GesHttpResponse r = new ObjectMapper().registerModule(new JavaTimeModule()).configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
                        .readerFor(HttpGesEventStreamReader.GesHttpResponse.class).readValue(jsonNode);

                return r.entries.stream()
                        .sorted((e1, e2) -> Long.compare(e1.positionEventNumber, e2.positionEventNumber))
                        .filter(e -> !e.streamId.startsWith("$"))
                        .map(e -> new ResolvedEvent(
                                new HttpGesEventStreamReader.GesHttpPosition(e.positionEventNumber),
                                eventRecord(
                                        e.updated,
                                        e.streamId(),
                                        e.eventNumber,
                                        e.eventType,
                                        e.data(),
                                        e.metaData())));
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }    }

    @Nonnull
    @Override
    public Position emptyStorePosition() {
        return null;
    }

    @Nonnull
    @Override
    public PositionCodec positionCodec() {
        return HttpGesEventStreamReader.GesHttpPosition.CODEC;
    }

    @Override
    public String toString() {
        return "HttpGesEventReader{" +
                "host='" + host + '\'' +
                '}';
    }
}
