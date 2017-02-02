package com.timgroup.eventstore.mysql.legacy;

import com.timgroup.eventstore.api.NewEvent;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class LegacyMysqlMetadataCodec {

    private static final Pattern EFFECTIVE_TIMESTAMP_PATTERN = Pattern.compile("\"effective_timestamp\"\\s*:\\s*\"([^\"]+)\"");
    private static final String EFFECTIVE_TIMESTAMP_FORMAT = "{\"effective_timestamp\":\"%s\"}";

    public static Timestamp effectiveTimestampFrom(NewEvent event) {
        if (event.metadata().length > 0) {
            String metadata = new String(event.metadata(), UTF_8);
            Matcher matcher = EFFECTIVE_TIMESTAMP_PATTERN.matcher(metadata);
            if (matcher.find()) {
                return new Timestamp(Instant.parse(matcher.group(1)).toEpochMilli());
            }
        }
        return new Timestamp(Instant.now().toEpochMilli());
    }

    public static byte[] metadataFrom(Timestamp effectiveTimestamp) {
        return String.format(EFFECTIVE_TIMESTAMP_FORMAT, effectiveTimestamp.toInstant()).getBytes(UTF_8);
    }
}
