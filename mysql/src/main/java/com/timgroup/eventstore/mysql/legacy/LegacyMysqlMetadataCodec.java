package com.timgroup.eventstore.mysql.legacy;

import com.timgroup.eventstore.api.NewEvent;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

final class LegacyMysqlMetadataCodec {

    private LegacyMysqlMetadataCodec() { /* prevent instantiation */ }

    private static final Pattern EFFECTIVE_TIMESTAMP_PATTERN = Pattern.compile("\"effective_timestamp\"\\s*:\\s*\"([^\"]+)\"");

    static Timestamp effectiveTimestampFrom(NewEvent event) {
        if (event.metadata().length > 0) {
            String metadata = new String(event.metadata(), UTF_8);
            Matcher matcher = EFFECTIVE_TIMESTAMP_PATTERN.matcher(metadata);
            if (matcher.find()) {
                return new Timestamp(Instant.parse(matcher.group(1)).toEpochMilli());
            }
        }
        return new Timestamp(Instant.now().toEpochMilli());
    }

    static byte[] metadataFrom(Timestamp effectiveTimestamp) {
        return ("{\"effective_timestamp\":\"" + effectiveTimestamp.toInstant() + "\"}").getBytes(UTF_8);
    }
}
