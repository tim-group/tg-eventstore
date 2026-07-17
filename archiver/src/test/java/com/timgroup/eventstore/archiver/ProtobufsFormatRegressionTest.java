package com.timgroup.eventstore.archiver;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class ProtobufsFormatRegressionTest {

    @Test
    public void extracts_message_from_reference_input() throws IOException {
        Instant instant = Instant.parse("2020-02-04T14:27:06.123456789Z");
        String referenceData = "gAAAAAgBEgsIuv7l8QUQlZrvOhoEdXNlciIkM0U0RTM3MUUtRTZBNC00MjlELUExM0MtOTIwQkZD\n" +
                "Q0I3QTVDKAEyC1VzZXJDcmVhdGVkOhZ7Im5hbWUiOiJGcmVkIEJsb2dncyJ9Qhx7Imhvc3QiOiJ1\n" +
                "c2VybWdtdC0wMDEucHJvZCJ9\n";
        List<EventStoreArchiverProtos.Event> results = new ArrayList<>();
        try (InputStream is = new ByteArrayInputStream(Base64.getMimeDecoder().decode(referenceData))) {
            new ProtobufsEventIterator<>(EventStoreArchiverProtos.Event.parser(), is).forEachRemaining(results::add);
        }
        assertThat(results, equalTo(ImmutableList.of(
                EventStoreArchiverProtos.Event.newBuilder()
                        .setPosition(1)
                        .setTimestamp(
                                EventStoreArchiverProtos.Timestamp.newBuilder()
                                        .setSeconds(instant.getEpochSecond())
                                        .setNanos(instant.getNano())
                                        .build()
                        )
                        .setStreamCategory("user")
                        .setStreamId("3E4E371E-E6A4-429D-A13C-920BFCCB7A5C")
                        .setEventNumber(1)
                        .setEventType("UserCreated")
                        .setData(ByteString.copyFrom("{\"name\":\"Fred Bloggs\"}", UTF_8))
                        .setMetadata(ByteString.copyFrom("{\"host\":\"usermgmt-001.prod\"}", UTF_8))
                        .build()
        )));
    }
}
