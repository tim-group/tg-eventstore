package com.timgroup.eventstore.filesystem;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.api.WrongExpectedVersionException;

final class FlatFilesystemEventStreamWriter implements EventStreamWriter {
    private final Path directory;
    private final Clock clock;
    private String dataSuffix;
    private String metadataSuffix;

    FlatFilesystemEventStreamWriter(Path directory, Clock clock, String filenameSuffix) {
        this.directory = directory;
        this.clock = clock;
        this.dataSuffix = ".data" + filenameSuffix;
        this.metadataSuffix = ".metadata" + filenameSuffix;
    }

    @Override
    public void write(StreamId streamId, Collection<NewEvent> events) {
        try (Lockfile ignored = lock(streamId)) {
            long currentVersion = currentVersion(streamId);
            writeImpl(streamId, events, currentVersion);
        }
    }

    @Override
    public void write(StreamId streamId, Collection<NewEvent> events, long expectedVersion) {
        try (Lockfile ignored = lock(streamId)) {
            long currentVersion = currentVersion(streamId);
            if (currentVersion != expectedVersion) {
                throw new WrongExpectedVersionException(currentVersion, expectedVersion);
            }
            writeImpl(streamId, events, currentVersion);
        }
    }

    private void writeImpl(StreamId streamId, Collection<NewEvent> events, long currentVersion) {
        try (Lockfile ignored = lock()) {
            long globalNumber = currentGlobalVersion() + 1;
            long eventNumber = currentVersion + 1;
            for (NewEvent newEvent : events) {
                Instant timestamp = Instant.now(clock);
                String filenamePrefix = FilenameCodec.format(globalNumber, timestamp, streamId, eventNumber, newEvent.type());
                Path dataFilename = directory.resolve(filenamePrefix + dataSuffix);
                Path metadataFilename = directory.resolve(filenamePrefix + metadataSuffix);
                try {
                    Files.write(dataFilename, newEvent.data());
                    if (newEvent.metadata().length != 0) {
                        Files.write(metadataFilename, newEvent.metadata());
                    }
                } catch (IOException e) {
                    try {
                        Files.deleteIfExists(dataFilename);
                        Files.deleteIfExists(metadataFilename);
                    } catch (IOException e1) {
                        // ignore
                    }
                    throw new RuntimeException("Unable to write event to " + streamId + ": " + newEvent, e);
                }
                ++eventNumber;
                ++globalNumber;
            }
        }
    }

    private long currentVersion(StreamId streamId) {
        try (Stream<Path> stream = Files.list(directory)) {
            return stream
                    .filter(p -> p.getFileName().toString().endsWith(dataSuffix))
                    .filter(p -> FilenameCodec.parse(p, (timestamp, fileStreamId, eventNumber, eventType) -> fileStreamId.equals(streamId)))
                    .count() - 1;
        } catch (IOException e) {
            throw new RuntimeException("Unable to count files in " + directory);
        }
    }

    private long currentGlobalVersion() {
        try (Stream<Path> stream = Files.list(directory)) {
            return stream.filter(p -> p.getFileName().toString().endsWith(dataSuffix)).count();
        } catch (IOException e) {
            throw new RuntimeException("Unable to count files in " + directory);
        }
    }

    private Lockfile lock(StreamId streamId) {
        Path lockFile = directory.resolve(String.format(".stream-lock.%s.%s", streamId.category(), streamId.id()));
        while (!createLockFile(lockFile)) {
            try {
                TimeUnit.MILLISECONDS.sleep(10L);
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted while locking stream");
            }
        }
        return new Lockfile(lockFile);
    }

    private Lockfile lock() {
        Path lockFile = directory.resolve(".global-lock");
        while (!createLockFile(lockFile)) {
            try {
                TimeUnit.MILLISECONDS.sleep(10L);
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted while locking store");
            }
        }
        return new Lockfile(lockFile);
    }

    private boolean createLockFile(Path file) {
        try {
            Files.createFile(file);
            return true;
        } catch (FileAlreadyExistsException e) {
            return false;
        } catch (IOException e) {
            throw new RuntimeException("Unable to create lock file: " + file, e);
        }
    }

    private final class Lockfile implements AutoCloseable {
        private final Path lockFile;

        Lockfile(Path lockFile) {
            this.lockFile = lockFile;
        }

        @Override
        public void close() {
            try {
                Files.delete(lockFile);
            } catch (IOException e) {
                throw new RuntimeException("Unable to delete lock file: " + lockFile, e);
            }
        }
    }
}
