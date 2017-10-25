package com.timgroup.eventstore.filesystem;

import java.nio.file.Path;
import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.timgroup.eventstore.api.StreamId;

import static com.timgroup.eventstore.api.StreamId.streamId;

final class FilenameCodec {
    private static final Pattern DATA_FILENAME_PATTERN = Pattern.compile("[0-9A-Fa-f]+\\.(\\d+-\\d+-\\d+T\\d+:\\d+:\\d+(?:\\.\\d+)?Z)\\.([^.]+)\\.([^.]+)\\.([^.]+)\\.([^.]+)\\.data");

    interface Receiver<T> {
        T accept(Instant timestamp, StreamId streamId, long eventNumber, String eventType);
    }

    static <T> T parse(Path dataPath, Receiver<? extends T> receiver) {
        Matcher matcher = FilenameCodec.DATA_FILENAME_PATTERN.matcher(dataPath.getFileName().toString());
        if (!matcher.lookingAt()) {
            throw new RuntimeException("Invalid data filename: " + dataPath);
        }

        int n = 0;
        Instant timestamp = Instant.parse(matcher.group(++n));
        String category = unescape(matcher.group(++n));
        String id = unescape(matcher.group(++n));
        long eventNumber = Long.parseLong(matcher.group(++n));
        String eventType = unescape(matcher.group(++n));

        return receiver.accept(timestamp, streamId(category, id), eventNumber, eventType);
    }

    static String format(long globalNumber, Instant timestamp, StreamId streamId, long eventNumber, String eventType) {
        return String.format("%08x.%s.%s.%s.%d.%s", globalNumber, timestamp, escape(streamId.category()), escape(streamId.id()), eventNumber, escape(eventType));
    }

    private static String escape(CharSequence input) {
        StringBuilder builder = new StringBuilder(input.length());
        for (int i = 0; i < input.length(); i++) {
            char ch = input.charAt(i);
            if (permitted(ch)) {
                builder.append(ch);
            }
            else {
                builder.append('%');
                builder.append(String.format("%04x", (int) ch));
            }
        }
        return builder.toString();
    }

    private static String unescape(CharSequence input) {
        StringBuilder builder = new StringBuilder(input.length());
        for (int i = 0; i < input.length(); i++) {
            char ch = input.charAt(i);
            if (ch == '%') {
                builder.append((char) Integer.parseInt(input.subSequence(i + 1, i + 5).toString(), 16));
                i += 4;
            }
            else {
                builder.append(ch);
            }
        }
        return builder.toString();
    }

    private static boolean permitted(char ch) {
        return ch != '%' && ch != '.' && ch > 32 && ch < 127;
    }

    private FilenameCodec() {
    }
}
