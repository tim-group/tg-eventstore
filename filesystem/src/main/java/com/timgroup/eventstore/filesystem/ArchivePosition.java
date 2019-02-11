package com.timgroup.eventstore.filesystem;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;

import javax.annotation.Nonnull;
import java.util.Objects;

final class ArchivePosition implements Comparable<ArchivePosition>, Position {
    static final ArchivePosition EMPTY = new ArchivePosition("");

    static final PositionCodec CODEC = PositionCodec.ofComparable(ArchivePosition.class,
            ArchivePosition::new,
            ArchivePosition::getFilename
    );

    @Nonnull
    private final String filename;

    public ArchivePosition(String filename) {
        this.filename = filename;
    }

    @Nonnull
    public String getFilename() {
        return filename;
    }

    @Override
    public int compareTo(@Nonnull ArchivePosition o) {
        return filename.compareTo(o.filename);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ArchivePosition that = (ArchivePosition) o;
        return filename.equals(that.filename);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename);
    }

    @Override
    public String toString() {
        return "ArchivePosition{" +
                "filename='" + filename + '\'' +
                '}';
    }
}
