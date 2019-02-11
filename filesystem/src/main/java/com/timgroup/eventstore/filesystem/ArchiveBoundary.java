package com.timgroup.eventstore.filesystem;

import com.timgroup.eventstore.api.Position;

import javax.annotation.Nonnull;
import java.util.Objects;

public final class ArchiveBoundary {
    @Nonnull
    private final Position inputPosition;
    @Nonnull
    private final Position archivePosition;
    private final long eventIndex;

    public ArchiveBoundary(@Nonnull Position inputPosition, @Nonnull Position archivePosition, long eventIndex) {
        this.inputPosition = inputPosition;
        this.archivePosition = archivePosition;
        this.eventIndex = eventIndex;
    }

    @Nonnull
    public Position getInputPosition() {
        return inputPosition;
    }

    @Nonnull
    public Position getArchivePosition() {
        return archivePosition;
    }

    public long getEventIndex() {
        return eventIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ArchiveBoundary that = (ArchiveBoundary) o;
        return eventIndex == that.eventIndex &&
                inputPosition.equals(that.inputPosition) &&
                archivePosition.equals(that.archivePosition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputPosition, archivePosition, eventIndex);
    }

    @Override
    public String toString() {
        return "ArchiveBoundary{" +
                "inputPosition=" + inputPosition +
                ", archivePosition=" + archivePosition +
                ", eventIndex=" + eventIndex +
                '}';
    }
}
