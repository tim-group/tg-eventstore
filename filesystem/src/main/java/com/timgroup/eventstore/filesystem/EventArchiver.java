package com.timgroup.eventstore.filesystem;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.EventStreamReader;
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
import java.util.stream.Stream;

public final class EventArchiver {
    @Nonnull
    private final EventReader storeReader;
    @Nonnull
    private final EventCategoryReader categoryReader;
    @Nonnull
    private final EventStreamReader streamReader;

    public EventArchiver(EventSource eventSource) {
        this.storeReader = eventSource.readAll();
        this.categoryReader = eventSource.readCategory();
        this.streamReader = eventSource.readStream();
    }

    public void archiveStore(Path outputFile) throws IOException {
        try (OutputStream stream = buffered(Files.newOutputStream(outputFile))) {
            archiveStore(stream);
        }
    }

    public void archiveStore(OutputStream output) throws IOException {
        try (Stream<ResolvedEvent> input = storeReader.readAllForwards()) {
            archiveEvents(input, output);
        }
    }

    public void archiveCategory(Path outputFile, String category) throws IOException {
        try (OutputStream stream = buffered(Files.newOutputStream(outputFile))) {
            archiveCategory(stream, category);
        }
    }

    public void archiveCategory(OutputStream output, String category) throws IOException {
        try (Stream<ResolvedEvent> input = categoryReader.readCategoryForwards(category)) {
            archiveEvents(input, output);
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

    private void archiveEvents(Stream<ResolvedEvent> input, OutputStream output) throws IOException {
        try (CpioArchiveOutputStream cpioOutput = new CpioArchiveOutputStreamWithoutNames(output)) {
            final long[] fileIndex = {0};
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
                } catch (IOException e) {
                    throw new WrappedIOException(e);
                }
            });
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

    private static final class WrappedIOException extends RuntimeException {
        @Nonnull private final IOException ioException;

        WrappedIOException(@Nonnull IOException e) {
            super(e);
            this.ioException = e;
        }

        @Nonnull
        public IOException getIoException() {
            return ioException;
        }
    }
}
