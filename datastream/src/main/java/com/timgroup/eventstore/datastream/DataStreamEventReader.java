package com.timgroup.eventstore.datastream;

import com.timgroup.eventstore.api.*;

import java.io.*;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.lang.Long.MAX_VALUE;
import static java.util.stream.StreamSupport.stream;

public class DataStreamEventReader implements EventReader {
    private final EventReader underlying;
    private final PositionCodec positionCodec;
    private final Path cacheDirectory;

    private static String cacheFileName = "cache";

    public DataStreamEventReader(EventReader underlying, PositionCodec positionCodec, Path cacheDirectory) {
        this.underlying = underlying;
        this.positionCodec = positionCodec;
        this.cacheDirectory = cacheDirectory;
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards() {
        Stream<ResolvedEvent> cachedEvents = readAllCachedEvents();
        try {
            File tmpOutputFile = File.createTempFile("cache", "inprogess", cacheDirectory.toFile());
            DataOutputStream output = new DataOutputStream(new FileOutputStream(tmpOutputFile));
            AtomicReference<Position> lastPosition = new AtomicReference(underlying.emptyStorePosition());

            Spliterator<ResolvedEvent> wrappedSpliterator = new Spliterator<ResolvedEvent>() {

                Spliterator<ResolvedEvent> underlyingSpliterator;
                @Override
                public boolean tryAdvance(Consumer<? super ResolvedEvent> action) {
                    if (underlyingSpliterator == null) {
                        underlyingSpliterator = underlying.readAllForwards(lastPosition.get()).spliterator();
                    }
                    boolean advanced = underlyingSpliterator.tryAdvance(event -> {
                        writeEvent(output, event);
                        action.accept(event);
                    });

                    if (!advanced) {
                        try {
                            output.close();
                            File dest = cacheDirectory.resolve(cacheFileName).toFile();
                            if (!dest.exists()) {
                                tmpOutputFile.renameTo(dest); // todo don't ignore rename
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                            // todo
                        }
                    }

                    return advanced;
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
            };
            return Stream.concat(cachedEvents.peek(resolvedEvent -> lastPosition.set(resolvedEvent.position())), StreamSupport.stream(wrappedSpliterator, false));
        } catch (FileNotFoundException e) {
            e.printStackTrace(); // todo
            throw new RuntimeException("Error reading cache", e);
        } catch (IOException e) {
            e.printStackTrace(); // todo
            throw new RuntimeException("Error reading cache", e);
        }
    }

    private void writeEvent(DataOutputStream output, ResolvedEvent resolvedEvent) {
        try {
            output.writeUTF(positionCodec.serializePosition(resolvedEvent.position()));
            EventRecord eventRecord = resolvedEvent.eventRecord();
            output.writeLong(eventRecord.timestamp().toEpochMilli());
            StreamId streamId = eventRecord.streamId();
            output.writeUTF(streamId.category());
            output.writeUTF(streamId.id());
            output.writeLong(eventRecord.eventNumber());
            output.writeUTF(eventRecord.eventType());
            output.writeInt(eventRecord.data().length);
            output.write(eventRecord.data());
            output.writeInt(eventRecord.metadata().length);
            output.write(eventRecord.metadata());
        } catch (IOException e) {
            e.printStackTrace(); // todo
        }
    }

    private Stream<ResolvedEvent> readAllCachedEvents() {
        Path cachePath = cacheDirectory.resolve(cacheFileName);
        File cacheFile = cachePath.toFile();
        Optional<Path> cachedFiles = cacheFile.exists() ? Optional.of(cacheFile.toPath()) : Optional.empty();
        Spliterator<ResolvedEvent> spliterator = new Spliterator<ResolvedEvent>() {
            private DataInputStream current = null;

            @Override
            public boolean tryAdvance(Consumer<? super ResolvedEvent> action) {
                if (current == null && cachedFiles.isPresent()) {
                    cachedFiles.ifPresent(path -> {
                        try {
                            current = new DataInputStream(new FileInputStream(path.toFile()));
                        } catch (FileNotFoundException e) {
                            e.printStackTrace(); // todo
                        }
                    });
                }
                if (current != null) {
                    return readNextEvent(action);
                } else {
                    return false;
                }
            }

            private boolean readNextEvent(Consumer<? super ResolvedEvent> action) {
                try {
                    ResolvedEvent resolvedEvent = new ResolvedEvent(positionCodec.deserializePosition(current.readUTF()),
                            EventRecord.eventRecord(Instant.ofEpochMilli(current.readLong()),
                                    StreamId.streamId(current.readUTF(), current.readUTF()),
                                    current.readLong(),
                                    current.readUTF(),
                                    readByteArray(current),
                                    readByteArray(current)));
                    action.accept(resolvedEvent);
                    return true;
                } catch (EOFException e) {
                    // todo: this should only be OK if throw from first read (position reading)
                    return false;
                } catch (IOException e) {
                    e.printStackTrace(); // todo
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

        };
        return stream(spliterator, false);
    }

    private Stream<Path> findAllCachedFiles() {
        try {
            return Files.find(cacheDirectory, 1, (file, attributes) -> true);
        } catch (IOException e) {
            e.printStackTrace(); // todo
            throw new RuntimeException("Error finding cache files", e);
        }
    }

    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        return null;
    }

    @Override
    public Position emptyStorePosition() {
        return null;
    }
}