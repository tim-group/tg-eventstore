package com.timgroup.eventstore.cache;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;

import java.io.File;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static java.util.stream.StreamSupport.stream;

/**
 * Reads events from cache file and then an underylying event reader
 */
public class CacheEventReader implements EventReader {
    private final EventReader underlying;
    private final PositionCodec positionCodec;
    private final Path cacheDirectory;

    private static String cacheFileName = "cache.gz";

    public CacheEventReader(EventReader underlying, PositionCodec positionCodec, Path cacheDirectory) {
        this.underlying = underlying;
        this.positionCodec = positionCodec;
        this.cacheDirectory = cacheDirectory;
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards() {
        Path cachePath = cacheDirectory.resolve(cacheFileName);
        File cacheFile = cachePath.toFile();
        Optional<Path> cachedFiles = cacheFile.exists() ? Optional.of(cacheFile.toPath()) : Optional.empty();
        return stream(new ReadCacheSpliterator(positionCodec, cachedFiles, maybePosition -> underlying.readAllForwards(maybePosition.orElse(underlying.emptyStorePosition()))), false);
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
        ReadCacheSpliterator spliterator = new ReadCacheSpliterator(positionCodec, Optional.of(cacheFile), ignore -> Stream.empty());
        AtomicReference<Position> lastPosition = new AtomicReference<>(null);
        spliterator.forEachRemaining(r -> lastPosition.set(r.position()));
        return Optional.ofNullable(lastPosition.get());
    }

}
