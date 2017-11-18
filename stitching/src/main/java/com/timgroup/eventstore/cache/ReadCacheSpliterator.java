package com.timgroup.eventstore.cache;

import com.timgroup.eventstore.api.EventRecord;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.lang.Long.MAX_VALUE;

class ReadCacheSpliterator implements Spliterator<ResolvedEvent> {
    private final PositionCodec positionCodec;
    private final LinkedList<Supplier<DataInputStream>> cachedFiles;
    private final Function<Optional<Position>, Stream<ResolvedEvent>> nextSupplier;

    private DataInputStream currentCache = null;
    private boolean cacheMayHaveMoreData = false;
    private Position lastPosition;
    private Spliterator<ResolvedEvent> underlyingSpliterator;

    public ReadCacheSpliterator(PositionCodec positionCodec,
                                List<Supplier<DataInputStream>> cachedFiles,
                                Function<Optional<Position>, Stream<ResolvedEvent>> nextSupplier) {
        this.nextSupplier = nextSupplier;
        this.positionCodec = positionCodec;
        this.cachedFiles = new LinkedList<>(cachedFiles);
    }

    @Override
    public boolean tryAdvance(Consumer<? super ResolvedEvent> action) {
        loadNextCache();
        if (cacheMayHaveMoreData) {
            try {
                ResolvedEvent resolvedEvent = readNextEvent();
                if (resolvedEvent != null) {
                    action.accept(resolvedEvent);
                    this.lastPosition = resolvedEvent.position();
                } else {
                    this.cacheMayHaveMoreData = false;
                }
                return this.cacheMayHaveMoreData;
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

    private void loadNextCache() {
        if (currentCache == null && !cachedFiles.isEmpty()) {
            Supplier<DataInputStream> cacheSupplier = cachedFiles.removeFirst();
            currentCache = cacheSupplier.get();
            cacheMayHaveMoreData = true;
        }
    }

    private ResolvedEvent readNextEvent() throws IOException {
        ResolvedEvent resolvedEvent = null;
        do {
            try {
                resolvedEvent = tryReadNextEvent();
            } catch (EOFException ignored) {
                // todo: this should only be OK if throw from first read (position reading)
                try {
                    currentCache.close();
                } catch (IOException swallowed) {
                    // ignore this as we need to continue processing the other cachedFiles
                }
                currentCache = null;
                if (!cachedFiles.isEmpty()) {
                    loadNextCache();
                }
            }
        } while (resolvedEvent == null && currentCache != null);

        return resolvedEvent;
    }

    private ResolvedEvent tryReadNextEvent() throws IOException {
        return new ResolvedEvent(positionCodec.deserializePosition(currentCache.readUTF()),
                EventRecord.eventRecord(Instant.ofEpochMilli(currentCache.readLong()),
                        StreamId.streamId(currentCache.readUTF(), currentCache.readUTF()),
                        currentCache.readLong(),
                        currentCache.readUTF(),
                        readByteArray(currentCache),
                        readByteArray(currentCache)));
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