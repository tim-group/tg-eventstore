package com.timgroup.eventstore.cache;

import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static java.lang.Long.MAX_VALUE;

class ReadCacheSpliterator implements Spliterator<ResolvedEvent> {
    private final PositionCodec positionCodec;
    private final Optional<Path> cachedFiles;
    private final Function<Optional<Position>, Stream<ResolvedEvent>> nextSupplier;

    private DataInputStream cache = null;
    private boolean cacheMayHaveMoreData = false;
    private Position lastPosition;
    private Spliterator<ResolvedEvent> underlyingSpliterator;

    public ReadCacheSpliterator(PositionCodec positionCodec,
                                Optional<Path> cachedFiles,
                                Function<Optional<Position>, Stream<ResolvedEvent>> nextSupplier) {
        this.nextSupplier = nextSupplier;
        this.positionCodec = positionCodec;
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
                underlyingSpliterator = nextSupplier.apply(Optional.ofNullable(lastPosition)).spliterator();
            }
            return underlyingSpliterator.tryAdvance(action);
        }
    }

    private void loadCache() {
        if (cache == null) {
            cachedFiles.ifPresent(path -> {
                try {
                    cache = new DataInputStream(new GZIPInputStream(new FileInputStream(path.toFile())));
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