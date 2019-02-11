package com.timgroup.eventstore.filesystem;

import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.StreamId;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static com.timgroup.eventstore.api.StreamId.streamId;

public final class ArchiveFilenameContent {
    private static final Pattern BASENAME_REGEX = Pattern.compile("([0-9A-Fa-f]{8})\\.([^.]+)\\.([^.]+)\\.([0-9]+)\\.([^.]+)");
    private static final Pattern FILENAME_REGEX = Pattern.compile(BASENAME_REGEX.pattern() + "\\.((?:meta)?data)");

    public static ArchiveFilenameContent parseFilename(CharSequence filename) {
        Matcher matcher = FILENAME_REGEX.matcher(filename);
        if (!matcher.matches()) throw new IllegalArgumentException("Invalid member filename: " + filename);
        return new ArchiveFilenameContent(
                Long.parseLong(matcher.group(1), 16),
                FilenameCodec.unescape(matcher.group(2)).intern(),
                FilenameCodec.unescape(matcher.group(3)),
                Long.parseLong(matcher.group(4)),
                FilenameCodec.unescape(matcher.group(5)).intern(),
                matcher.group(6).intern(),
                filename.subSequence(0, matcher.start(6) - 1).toString().intern()
        );
    }

    public static ArchiveFilenameContent parseBasename(CharSequence basename, String extension) {
        Matcher matcher = BASENAME_REGEX.matcher(basename);
        if (!matcher.matches()) throw new IllegalArgumentException("Invalid member basename: " + basename);
        return new ArchiveFilenameContent(
                Long.parseLong(matcher.group(1), 16),
                FilenameCodec.unescape(matcher.group(2)).intern(),
                FilenameCodec.unescape(matcher.group(3)),
                Long.parseLong(matcher.group(4)),
                FilenameCodec.unescape(matcher.group(5)).intern(),
                extension,
                basename.toString()
        );
    }

    private final long position;
    @Nonnull private final String category;
    @Nonnull private final String id;
    private final long eventNumber;
    @Nonnull private final String eventType;
    @Nonnull private final String extension;
    @Nonnull private final String basename;

    public ArchiveFilenameContent(long position, String category, String id, long eventNumber, String eventType, String extension, String basename) {
        this.position = position;
        this.category = category;
        this.id = id;
        this.eventNumber = eventNumber;
        this.eventType = eventType;
        this.extension = extension;
        this.basename = basename;
    }

    public long getPosition() {
        return position;
    }

    @Nonnull
    public String getCategory() {
        return category;
    }

    @Nonnull
    public String getId() {
        return id;
    }

    public long getEventNumber() {
        return eventNumber;
    }

    @Nonnull
    public String getEventType() {
        return eventType;
    }

    @Nonnull
    public String getExtension() {
        return extension;
    }

    @Nonnull
    public String getBasename() {
        return basename;
    }

    @Nonnull
    public StreamId getStreamId() {
        return streamId(category, id);
    }

    @Nonnull
    public EventRecord toEventRecord(@Nonnull Instant timestamp, @Nonnull byte[] data, @Nonnull byte[] metadata) {
        return eventRecord(timestamp, getStreamId(), eventNumber, eventType, data, metadata);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ArchiveFilenameContent that = (ArchiveFilenameContent) o;
        return position == that.position &&
                eventNumber == that.eventNumber &&
                category.equals(that.category) &&
                id.equals(that.id) &&
                eventType.equals(that.eventType) &&
                extension.equals(that.extension) &&
                basename.equals(that.basename);
    }

    @Override
    public int hashCode() {
        return Objects.hash(position, category, id, eventNumber, eventType, extension, basename);
    }

    @Override
    public String toString() {
        return "ArchiveFilenameContent{" +
                "position=" + position +
                ", category='" + category + '\'' +
                ", id='" + id + '\'' +
                ", eventNumber=" + eventNumber +
                ", eventType='" + eventType + '\'' +
                ", extension='" + extension + '\'' +
                ", basename='" + basename + '\'' +
                '}';
    }
}
