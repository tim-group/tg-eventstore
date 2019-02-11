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
        Optional<ArchiveBoundary> lastArchiveBoundary = findLastArchiveBoundary();
        if (!lastArchiveBoundary.isPresent() && !createInitial) throw new IllegalStateException("No existing archives and createInitial not specified");
        Optional<ArchiveBoundary> archivedTo;
        Path tempFile = Files.createTempFile(archiveDirectory, "__archive", ".cpio.tmp");
        try (OutputStream tempFileOutput = Files.newOutputStream(tempFile)) {
            archivedTo = eventArchiver.archiveStore(tempFileOutput, lastArchiveBoundary.orElse(null));
        }
        if (!archivedTo.isPresent()) {
            Files.delete(tempFile);
        } else {
            String basename = ArchivePosition.CODEC.serializePosition(archivedTo.get().getArchivePosition());
            Files.write(tempFile.resolveSibling(basename + ".position.txt"), positionCodec.serializePosition(archivedTo.get().getInputPosition()).getBytes(UTF_8));
            Files.move(tempFile, tempFile.resolveSibling(basename + ".cpio"));
        }
    }

    private Optional<ArchiveBoundary> findLastArchiveBoundary() throws IOException {
        List<Path> positionFiles = listFiles(s -> s.endsWith(".position.txt"));
        if (positionFiles.isEmpty())
            return Optional.empty();
        Path file = positionFiles.get(positionFiles.size() - 1);
        Position inputPosition = positionCodec.deserializePosition(Files.readAllLines(file).get(0));
        Position archivePosition = ArchivePosition.CODEC.deserializePosition(file.getFileName().toString().replaceFirst("\\.position\\.txt$", ""));
        return Optional.of(new ArchiveBoundary(inputPosition, archivePosition, 0L));
    }

    private List<Path> listFiles(Predicate<? super String> filenameMatches) throws IOException {
        try (Stream<Path> paths = Files.list(archiveDirectory)) {
            return paths.filter(p -> filenameMatches.test(p.getFileName().toString()))
                    .sorted()
                    .collect(toList());
        }
    }
}
