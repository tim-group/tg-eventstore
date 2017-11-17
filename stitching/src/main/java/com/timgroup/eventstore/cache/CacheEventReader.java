package com.timgroup.eventstore.cache;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.cache.ReadCacheSpliterator.CacheNotFoundException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;

/**
 * Reads events from cache file and then an underylying event reader
 */
public class CacheEventReader implements EventReader {
    private final EventReader underlying;
    private final PositionCodec positionCodec;
    private final Path cacheDirectory;
    private final String cacheFileBaseName;

    public CacheEventReader(EventReader underlying, PositionCodec positionCodec, Path cacheDirectory, String cacheFileBaseName) {
        this.underlying = underlying;
        this.positionCodec = positionCodec;
        this.cacheDirectory = cacheDirectory;
        this.cacheFileBaseName = cacheFileBaseName;
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards() {
        List<Path> cacheList = getCacheList();
        return stream(new ReadCacheSpliterator(positionCodec, cacheList,
                maybePosition -> {
                    Position position = maybePosition.orElse(underlying.emptyStorePosition());
                    return underlying.readAllForwards(position);
                }), false);
    }

    private List<Path> getCacheList() {
        try {
            return Files.list(cacheDirectory)
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().startsWith(cacheFileBaseName))
                    .filter(path -> !path.getFileName().toString().endsWith(".tmp"))
                    .sorted()
                    .collect(toList());
        } catch (IOException e) {
            throw new CacheReadingException("Unable to get cache files from cacheDirectory: " + cacheDirectory, e);
        }
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        if (positionExclusive.equals(emptyStorePosition())) {
            return readAllForwards();
        } else {
            return underlying.readAllForwards(positionExclusive);
        }
    }

    @Override
    public Position emptyStorePosition() {
        return underlying.emptyStorePosition();
    }

    public static Optional<Position> findLastPosition(Path cacheFile, PositionCodec positionCodec) {
        ReadCacheSpliterator spliterator = new ReadCacheSpliterator(positionCodec, singletonList(cacheFile), ignore -> Stream.empty());
        AtomicReference<Position> lastPosition = new AtomicReference<>(null);
        try {
            spliterator.forEachRemaining(r -> lastPosition.set(r.position()));
            return Optional.ofNullable(lastPosition.get());
        } catch (CacheNotFoundException e) {
            return Optional.empty();
        }
    }

    public static class CacheReadingException extends RuntimeException {
        public CacheReadingException(String reason, Exception cause) {
            super(reason, cause);
        }
    }
}
