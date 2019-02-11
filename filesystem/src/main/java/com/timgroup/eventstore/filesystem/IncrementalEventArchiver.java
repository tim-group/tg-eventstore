package com.timgroup.eventstore.filesystem;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class IncrementalEventArchiver {
    @Nonnull
    private final Path archiveDirectory;
    private final boolean createInitial;
    @Nonnull
    private final EventReader storeReader;
    @Nonnull
    private final PositionCodec positionCodec;
    @Nonnull
    private final EventArchiver eventArchiver;

    public IncrementalEventArchiver(@Nonnull EventSource eventSource, @Nonnull Path archiveDirectory, boolean createInitial) {
        this.storeReader = eventSource.readAll();
        this.positionCodec = eventSource.positionCodec();
        this.archiveDirectory = requireNonNull(archiveDirectory);
        this.createInitial = createInitial;
        this.eventArchiver = new EventArchiver(eventSource);
    }

    public void archiveEvents() throws IOException {
        Optional<Position> lastPositionOptional = findLastSourcePosition();
        if (!lastPositionOptional.isPresent() && !createInitial) throw new IllegalStateException("No existing archives and createInitial not specified");
        Position lastPosition = lastPositionOptional.orElseGet(storeReader::emptyStorePosition);
        Optional<ArchiveBoundary> archivedTo;
        Path tempFile = Files.createTempFile(archiveDirectory, "__archive", ".cpio.tmp");
        try (OutputStream tempFileOutput = Files.newOutputStream(tempFile)) {
            archivedTo = eventArchiver.archiveStore(tempFileOutput);
        }
        if (!archivedTo.isPresent()) {
            Files.delete(tempFile);
        } else {
            String basename = ArchivePosition.CODEC.serializePosition(archivedTo.get().getArchivePosition());
            Files.write(tempFile.resolveSibling(basename + ".position.txt"), positionCodec.serializePosition(archivedTo.get().getInputPosition()).getBytes(UTF_8));
            Files.move(tempFile, tempFile.resolveSibling(basename + ".cpio"));
        }
    }

    private Optional<Position> findLastSourcePosition() throws IOException {
        List<Path> positionFiles = listFiles(s -> s.endsWith(".position.txt"));
        if (positionFiles.isEmpty())
            return Optional.empty();
        return Optional.of(positionCodec.deserializePosition(Files.readAllLines(positionFiles.get(positionFiles.size() - 1)).get(0)));
    }

    private List<Path> listFiles(Predicate<? super String> filenameMatches) throws IOException {
        try (Stream<Path> paths = Files.list(archiveDirectory)) {
            return paths.filter(p -> filenameMatches.test(p.getFileName().toString()))
                    .sorted()
                    .collect(toList());
        }
    }
}
