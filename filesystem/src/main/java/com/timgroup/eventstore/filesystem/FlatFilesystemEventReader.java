package com.timgroup.eventstore.filesystem;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static java.util.Comparator.comparing;

@ParametersAreNonnullByDefault
final class FlatFilesystemEventReader implements EventReader {
    private static final byte[] EMPTY_METADATA = new byte[0];
    private final Path directory;
    private final String dataSuffix;
    private final String metadataSuffix;

    FlatFilesystemEventReader(Path directory, String filenameSuffix) {
        this.directory = directory;
        this.dataSuffix = ".data" + filenameSuffix;
        this.metadataSuffix = ".metadata" + filenameSuffix;
    }

    @Nonnull
    @CheckReturnValue
    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        String afterFilename = ((FilesystemPosition) positionExclusive).getFilename();
        try {
            return Files.list(directory)
                    .filter(p -> p.getFileName().toString().endsWith(dataSuffix))
                    .filter(p -> p.getFileName().toString().compareTo(afterFilename) > 0)
                    .sorted(comparing(Path::getFileName))
                    .map(this::readFile);
        } catch (IOException e) {
            throw new RuntimeException("Unable to list files in " + directory, e);
        }
    }

    @Nonnull
    @CheckReturnValue
    @Override
    public Stream<ResolvedEvent> readAllBackwards() {
        try {
            return Files.list(directory)
                    .filter(p -> p.getFileName().toString().endsWith(dataSuffix))
                    .sorted(comparing(Path::getFileName).reversed())
                    .map(this::readFile);
        } catch (IOException e) {
            throw new RuntimeException("Unable to list files in " + directory, e);
        }
    }

    @Nonnull
    @CheckReturnValue
    @Override
    public Stream<ResolvedEvent> readAllBackwards(Position positionExclusive) {
        String beforeFilename = ((FilesystemPosition) positionExclusive).getFilename();
        try {
            return Files.list(directory)
                    .filter(p -> p.getFileName().toString().endsWith(dataSuffix))
                    .filter(p -> p.getFileName().toString().compareTo(beforeFilename) < 0)
                    .sorted(comparing(Path::getFileName).reversed())
                    .map(this::readFile);
        } catch (IOException e) {
            throw new RuntimeException("Unable to list files in " + directory, e);
        }
    }

    boolean streamExists(StreamId streamId) {
        try (Stream<Path> stream = Files.list(directory)) {
            return stream
                    .filter(p -> p.getFileName().toString().endsWith(dataSuffix))
                    .anyMatch(p -> FilenameCodec.parse(p, (timestamp, fileStreamId, eventNumber, eventType) -> fileStreamId.equals(streamId)));
        } catch (IOException e) {
            throw new RuntimeException("Unable to count files in " + directory);
        }
    }

    private ResolvedEvent readFile(Path dataPath) {
        byte[] data, metadata;

        try {
            data = Files.readAllBytes(dataPath);
        } catch (IOException e) {
            throw new RuntimeException("Unable to read " + dataPath, e);
        }

        Path metadataPath = metadataPath(dataPath);
        try {
            if (Files.exists(metadataPath)) {
                metadata = Files.readAllBytes(metadataPath);
            }
            else {
                metadata = EMPTY_METADATA;
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to read " + metadataPath, e);
        }

        return FilenameCodec.parse(dataPath, (timestamp, streamId, eventNumber, eventType) -> eventRecord(timestamp, streamId, eventNumber, eventType, data, metadata))
                    .toResolvedEvent(new FilesystemPosition(dataPath.getFileName().toString()));
    }

    private Path metadataPath(Path dataPath) {
        String filename = dataPath.getFileName().toString();
        StringBuilder builder = new StringBuilder(filename);
        builder.setLength(builder.length() - dataSuffix.length());
        builder.append(metadataSuffix);
        return dataPath.resolveSibling(builder.toString());
    }

    @Nonnull
    @Override
    public Position emptyStorePosition() {
        return FilesystemPosition.EMPTY;
    }

    @Override
    public String toString() {
        return "FlatFilesystemEventReader{" +
                "directory=" + directory +
                ", dataSuffix='" + dataSuffix + '\'' +
                ", metadataSuffix='" + metadataSuffix + '\'' +
                '}';
    }
}
