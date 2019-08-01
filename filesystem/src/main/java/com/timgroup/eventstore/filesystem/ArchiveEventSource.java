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
import java.util.Collection;

import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;

public final class ArchiveEventSource implements EventSource {
    @Nonnull
    private final Path archivePath;

    public ArchiveEventSource(@Nonnull Path archivePath) {
        this.archivePath = requireNonNull(archivePath);
    }

    @Nonnull
    @Override
    public EventReader readAll() {
        return new ArchiveEventReader(archivePath);
    }

    @Nonnull
    @Override
    public EventCategoryReader readCategory() {
        return new FilteringCategoryReader(readAll());
    }

    @Nonnull
    @Override
    public EventStreamReader readStream() {
        return new FilteringStreamReader(readAll(), stream -> true);
    }

    @Nonnull
    @Override
    public EventStreamWriter writeStream() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    @Deprecated
    public PositionCodec positionCodec() {
        return ArchivePosition.CODEC;
    }

    @Nonnull
    @Override
    public Collection<Component> monitoring() {
        return emptySet();
    }
}
