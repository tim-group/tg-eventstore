package com.timgroup.eventstore.cache;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static java.lang.Long.MAX_VALUE;
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
        return stream(new ReadCacheSpliterator(underlying, positionCodec, cachedFiles), false);
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

    private static class ReadCacheSpliterator implements Spliterator<ResolvedEvent> {
        private static final int ONE_MEGABYTE = 1024 * 1024;

        private final EventReader underlying;
        private final PositionCodec positionCodec;
        private final Optional<Path> cachedFiles;

        private DataInputStream cache = null;
        private boolean cacheMayHaveMoreData = false;
        private Position lastPosition;
        private Spliterator<ResolvedEvent> underlyingSpliterator;

        public ReadCacheSpliterator(EventReader underlying,
                                    PositionCodec positionCodec,
                                    Optional<Path> cachedFiles) {
            this.underlying = underlying;
            this.positionCodec = positionCodec;
            this.lastPosition = underlying.emptyStorePosition();
            this.cachedFiles = cachedFiles;
        }

        @Override
        public boolean tryAdvance(Consumer<? super ResolvedEvent> action) {
            loadCache();
            if (cacheMayHaveMoreData) {
                try {
                    cacheMayHaveMoreData = readNextEvent(action);
                    return cacheMayHaveMoreData;
                } catch (IOException e) {
                    e.printStackTrace(); // todo
                    return false;
                }
            } else {
                if (underlyingSpliterator == null) {
                    underlyingSpliterator = underlying.readAllForwards(lastPosition).spliterator();
                }
                return underlyingSpliterator.tryAdvance(action);
            }
        }

        private void loadCache() {
            if (cache == null) {
                cachedFiles.ifPresent(path -> {
                    try {
                        cache = new DataInputStream(new GZIPInputStream(new FileInputStream(path.toFile()), ONE_MEGABYTE));
                        cacheMayHaveMoreData = true;
                    } catch (FileNotFoundException e) {
                        e.printStackTrace(); // todo
                    } catch (IOException e) {
                        e.printStackTrace(); // todo
                    }
                });
            }
        }

        private boolean readNextEvent(Consumer<? super ResolvedEvent> action) throws IOException {
            try {
                ResolvedEvent resolvedEvent = new ResolvedEvent(positionCodec.deserializePosition(cache.readUTF()),
                        EventRecord.eventRecord(Instant.ofEpochMilli(cache.readLong()),
                                StreamId.streamId(cache.readUTF(), cache.readUTF()),
                                cache.readLong(),
                                cache.readUTF(),
                                readByteArray(cache),
                                readByteArray(cache)));
                action.accept(resolvedEvent);
                lastPosition = resolvedEvent.position();
                return true;
            } catch (EOFException e) {
                cache.close();
                // todo: this should only be OK if throw from first read (position reading)
                return false;
            }
        }

        private byte[] readByteArray(DataInputStream current) throws IOException {
            int size = current.readInt();
            byte[] buffer = new byte[size];
            current.readFully(buffer);
            return buffer;
        }

        @Override
        public Spliterator<ResolvedEvent> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return MAX_VALUE;
        }

        @Override
        public int characteristics() {
            return ORDERED | NONNULL | DISTINCT;
        }

    }
}
