package com.timgroup.eventstore.filesystem;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;

import javax.annotation.Nullable;
import java.util.Objects;

final class FlatFilesystemPosition implements Position, Comparable<FlatFilesystemPosition> {
    static final PositionCodec CODEC = PositionCodec.ofComparable(FlatFilesystemPosition.class,
            FlatFilesystemPosition::parse,
            FlatFilesystemPosition::format);

    static final FlatFilesystemPosition EMPTY = new FlatFilesystemPosition("");

    private final String filename;

    public FlatFilesystemPosition(String filename) {
        this.filename = filename;
    }

    public static FlatFilesystemPosition parse(String input) {
        return new FlatFilesystemPosition(input);
    }

    public String format() {
        return filename;
    }

    String getFilename() {
        return filename;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlatFilesystemPosition that = (FlatFilesystemPosition) o;
        return Objects.equals(filename, that.filename);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename);
    }

    @Override
    public int compareTo(FlatFilesystemPosition o) {
        return filename.compareTo(o.filename);
    }

    @Override
    public String toString() {
        return filename;
    }
}
