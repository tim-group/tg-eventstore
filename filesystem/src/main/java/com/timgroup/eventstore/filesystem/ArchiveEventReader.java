package com.timgroup.eventstore.filesystem;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import org.apache.commons.compress.archivers.cpio.CpioArchiveEntry;
import org.apache.commons.compress.archivers.cpio.CpioArchiveInputStream;
import org.tukaani.xz.XZInputStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;

import static java.util.Objects.requireNonNull;

public class ArchiveEventReader implements EventReader {
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    @Nonnull
    private final Path archivePath;

    ArchiveEventReader(Path archivePath) {
        this.archivePath = requireNonNull(archivePath);
    }

    @Nonnull
    @Override
    public Stream<ResolvedEvent> readAllForwards(@Nonnull Position positionExclusive) {
        String seekToPosition = ((ArchivePosition) positionExclusive).getFilename();
        ArchiveEventIterator iterator = new ArchiveEventIterator(seekToPosition.isEmpty() ? null : seekToPosition + "~");
        Spliterator<ResolvedEvent> spliterator = Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED | Spliterator.DISTINCT | Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, false).onClose(iterator::close);
    }

    @Nonnull
    @Override
    public Position emptyStorePosition() {
        return ArchivePosition.EMPTY;
    }

    private InputStream openFile() throws IOException {
        String filename = archivePath.getFileName().toString();
        if (filename.endsWith(".cpio.xz")) {
            return new XZInputStream(Files.newInputStream(archivePath));
        } else if (filename.endsWith(".cpio.gz")) {
            return new GZIPInputStream(Files.newInputStream(archivePath));
        } else if (filename.endsWith(".cpio")) {
            return new BufferedInputStream(Files.newInputStream(archivePath));
        } else {
            throw new IllegalArgumentException("Unrecognised filename extension: " + archivePath);
        }
    }

    private final class ArchiveEventIterator extends AbstractIterator<ResolvedEvent> implements AutoCloseable {
        @Nonnull
        private final CpioArchiveInputStream cpioInput;
        @Nonnull
        private final CpioEntryBuffer buffer;
        @Nullable
        private String positionExclusive;

        ArchiveEventIterator(@Nullable String positionExclusive) {
            super();
            this.positionExclusive = positionExclusive;

            try {
                cpioInput = new CpioArchiveInputStream(openFile());
            } catch (IOException e) {
                throw new WrappedIOException(e);
            }

            buffer = new CpioEntryBuffer(cpioInput);
        }

        @Override
        public void computeNext() {
            try {
                if (positionExclusive != null) {
                    // seek start position
                    CpioArchiveEntry cpioEntry;
                    while ((cpioEntry = buffer.getNextEntry()) != null) {
                        if (cpioEntry.getName().compareTo(positionExclusive) > 0) {
                            break;
                        }
                        buffer.shift();
                        //noinspection ResultOfMethodCallIgnored
                        cpioInput.skip(cpioEntry.getSize());
                    }
                    positionExclusive = null;
                }

                CpioArchiveEntry cpioEntry = buffer.shiftOrNull();
                if (cpioEntry == null) {
                    done();
                    return;
                }

                ArchiveFilenameContent filenameContent = ArchiveFilenameContent.parseFilename(cpioEntry.getName());
                if (!filenameContent.getExtension().equals("data")) {
                    throw new IllegalStateException("Expected data member: " + cpioEntry.getName());
                }

                byte[] data = readNBytes((int) cpioEntry.getSize());
                byte[] metadata;
                CpioArchiveEntry nextEntry = buffer.getNextEntry();
                if (nextEntry != null && nextEntry.getName().endsWith(".metadata")) {
                    buffer.shift();
                    metadata = readNBytes((int) nextEntry.getSize());
                } else {
                    metadata = EMPTY_BYTE_ARRAY;
                }

                ResolvedEvent re = filenameContent
                        .toEventRecord(Instant.ofEpochMilli(cpioEntry.getTime()), data, metadata)
                        .toResolvedEvent(new ArchivePosition(filenameContent.getBasename()));
                setNext(re);
            } catch (IOException e) {
                throw new WrappedIOException(e);
            }
        }

        @Override
        public void close() {
            try {
                cpioInput.close();
            } catch (IOException e) {
                throw new WrappedIOException(e);
            }
        }

        private byte[] readNBytes(int length) throws IOException {
            byte[] output = new byte[length];
            int offset = 0;
            while (offset < output.length) {
                int got = cpioInput.read(output, offset, output.length - offset);
                if (got < 0)
                    throw new IOException("Short read, expected " + length + " bytes, got " + offset);
                offset += got;
            }
            return output;
        }
    }

    private static final class CpioEntryBuffer {
        @Nonnull
        private final CpioArchiveInputStream stream;
        private boolean loaded;
        @Nullable
        private CpioArchiveEntry bufferedEntry;

        CpioEntryBuffer(CpioArchiveInputStream stream) {
            this.stream = stream;
        }

        @Nonnull
        CpioArchiveEntry shift() throws IOException {
            loadNextEntry();
            CpioArchiveEntry shifted = bufferedEntry;
            if (shifted == null)
                throw new IllegalStateException("No entries remaining");
            loaded = false;
            bufferedEntry = null;
            return shifted;
        }

        @Nullable
        CpioArchiveEntry getNextEntry() throws IOException {
            loadNextEntry();
            return bufferedEntry;
        }

        @Nullable
        CpioArchiveEntry shiftOrNull() throws IOException {
            if (getNextEntry() == null)
                return null;
            return shift();
        }

        private void loadNextEntry() throws IOException {
            if (loaded) return;
            bufferedEntry = stream.getNextCPIOEntry();
            loaded = true;
        }
    }
}
