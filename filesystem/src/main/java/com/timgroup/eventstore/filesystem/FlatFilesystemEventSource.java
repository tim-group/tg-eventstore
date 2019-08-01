package com.timgroup.eventstore.filesystem;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.EventStreamWriter;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.tucker.info.Component;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Collection;
import java.util.Collections;

public final class FlatFilesystemEventSource implements EventSource {
    private final Path directory;
    private final Clock clock;
    private final String filenameSuffix;

    public FlatFilesystemEventSource(Path directory, Clock clock, String filenameSuffix) {
        this.directory = directory;
        this.clock = clock;
        this.filenameSuffix = filenameSuffix;
    }

    @Nonnull
    @Override
    public EventReader readAll() {
        return new FlatFilesystemEventReader(directory, filenameSuffix);
    }

    @Nonnull
    @Override
    public EventCategoryReader readCategory() {
        return new FilteringCategoryReader(readAll());
    }

    @Nonnull
    @Override
    public EventStreamReader readStream() {
        FlatFilesystemEventReader eventReader = new FlatFilesystemEventReader(directory, filenameSuffix);
        return new FilteringStreamReader(eventReader, eventReader::streamExists);
    }

    @Nonnull
    @Override
    public EventStreamWriter writeStream() {
        return new FlatFilesystemEventStreamWriter(directory, clock, filenameSuffix);
    }

    @Nonnull
    @Override
    @Deprecated
    public PositionCodec positionCodec() {
        return FlatFilesystemPosition.CODEC;
    }

    @Nonnull
    @Override
    public Collection<Component> monitoring() {
        return Collections.emptyList();
    }

    @Override
    public String toString() {
        return "FlatFilesystemEventSource{" +
                "directory=" + directory +
                ", clock=" + clock +
                ", filenameSuffix='" + filenameSuffix + '\'' +
                '}';
    }
}
