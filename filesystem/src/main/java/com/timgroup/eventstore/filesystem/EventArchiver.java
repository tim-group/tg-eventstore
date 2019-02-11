package com.timgroup.eventstore.filesystem;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.cpio.CpioArchiveEntry;
import org.apache.commons.compress.archivers.cpio.CpioArchiveOutputStream;

import javax.annotation.Nonnull;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class EventArchiver {
    @Nonnull
    private final EventReader storeReader;
    @Nonnull
    private final EventCategoryReader categoryReader;
    @Nonnull
    private final EventStreamReader streamReader;
    @Nonnull
    private final PositionCodec positionCodec;

    public EventArchiver(EventSource eventSource) {
        this.storeReader = eventSource.readAll();
        this.categoryReader = eventSource.readCategory();
        this.streamReader = eventSource.readStream();
        this.positionCodec = eventSource.positionCodec();
    }

    public void archiveStore(Path outputFile) throws IOException {
        try (OutputStream stream = buffered(Files.newOutputStream(outputFile))) {
            archiveStore(stream);
        }
    }

    public Optional<ArchiveBoundary> archiveStore(OutputStream output) throws IOException {
        try (Stream<ResolvedEvent> input = storeReader.readAllForwards()) {
            return archiveEvents(input, output);
        }
    }

    public void archiveCategory(Path outputFile, String category) throws IOException {
        try (OutputStream stream = buffered(Files.newOutputStream(outputFile))) {
            archiveCategory(stream, category);
        }
    }

    public Optional<ArchiveBoundary> archiveCategory(OutputStream output, String category) throws IOException {
        try (Stream<ResolvedEvent> input = categoryReader.readCategoryForwards(category)) {
            return archiveEvents(input, output);
        }
    }

    public void archiveStream(Path outputFile, StreamId streamId) throws IOException {
        try (OutputStream stream = buffered(Files.newOutputStream(outputFile))) {
            archiveStream(stream, streamId);
        }
    }

    public void archiveStream(OutputStream output, StreamId streamId) throws IOException {
        try (Stream<ResolvedEvent> input = streamReader.readStreamForwards(streamId)) {
            archiveEvents(input, output);
        }
    }

    private void writeString(Path outputFile, String content) throws IOException {
        Files.write(outputFile, content.getBytes(UTF_8));
    }

    private Optional<ArchiveBoundary> archiveEvents(Stream<ResolvedEvent> input, OutputStream output) throws IOException {
        try (CpioArchiveOutputStream cpioOutput = new CpioArchiveOutputStreamWithoutNames(output)) {
            final long[] fileIndex = {0};
            final Position[] position = {null};
            final String[] lastBasename = {null};
            input.forEachOrdered(re -> {
                try {
                    EventRecord eventRecord = re.eventRecord();
                    String basename = String.join(".",
                            String.format("%08x", fileIndex[0]),
                            FilenameCodec.escape(eventRecord.streamId().category()),
                            FilenameCodec.escape(eventRecord.streamId().id()),
                            Long.toString(eventRecord.eventNumber()),
                            FilenameCodec.escape(eventRecord.eventType())
                    );
                    writeEntry(cpioOutput, basename + ".data", eventRecord.data(), eventRecord.timestamp());
                    if (eventRecord.metadata().length > 0) {
                        writeEntry(cpioOutput, basename + ".metadata", eventRecord.metadata(), eventRecord.timestamp());
                    }
                    ++fileIndex[0];
                    position[0] = re.position();
                    lastBasename[0] = basename;
                } catch (IOException e) {
                    throw new WrappedIOException(e);
                }
            });
            return Optional.ofNullable(position[0]).map(inputPosition -> new ArchiveBoundary(inputPosition, new ArchivePosition(lastBasename[0]), fileIndex[0]));
        } catch (WrappedIOException e) {
            throw e.getIoException();
        }
    }

    private static void writeEntry(CpioArchiveOutputStream cpioOutput, String filename, byte[] content, Instant timestamp) throws IOException {
        CpioArchiveEntry entry = new CpioArchiveEntry(filename);
        entry.setSize(content.length);
        entry.setTime(timestamp.toEpochMilli());
        cpioOutput.putArchiveEntry(entry);
        cpioOutput.write(content);
        cpioOutput.closeArchiveEntry();
    }

    private static BufferedOutputStream buffered(OutputStream underlying) {
        if (underlying instanceof BufferedOutputStream) {
            return (BufferedOutputStream) underlying;
        } else {
            return new BufferedOutputStream(underlying);
        }
    }

    private static final class CpioArchiveOutputStreamWithoutNames extends CpioArchiveOutputStream {
        private static final Field namesMapField;

        static {
            try {
                namesMapField = CpioArchiveOutputStream.class.getDeclaredField("names");
            } catch (NoSuchFieldException e) {
                throw new RuntimeException(e);
            }
            namesMapField.setAccessible(true);
        }

        private final Map<?, ?> namesMap;

        CpioArchiveOutputStreamWithoutNames(OutputStream underlying) {
            super(underlying);
            try {
                namesMap = (Map<?, ?>) namesMapField.get(this);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void putArchiveEntry(ArchiveEntry entry) throws IOException {
            super.putArchiveEntry(entry);
            namesMap.remove(entry.getName());
        }
    }
}
