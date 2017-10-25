package com.timgroup.eventstore.filesystem;

import java.util.Objects;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;

final class FilesystemPosition implements Position, Comparable<FilesystemPosition> {
    static final PositionCodec CODEC = PositionCodec.ofComparable(FilesystemPosition.class,
            FilesystemPosition::parse,
            FilesystemPosition::format);

    static final FilesystemPosition EMPTY = new FilesystemPosition("");

    private final String filename;

    public FilesystemPosition(String filename) {
        this.filename = filename;
    }

    public static FilesystemPosition parse(String input) {
        return new FilesystemPosition(input);
    }

    public String format() {
        return filename;
    }

    String getFilename() {
        return filename;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FilesystemPosition that = (FilesystemPosition) o;
        return Objects.equals(filename, that.filename);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename);
    }

    @Override
    public int compareTo(FilesystemPosition o) {
        return filename.compareTo(o.filename);
    }

    @Override
    public String toString() {
        return filename;
    }
}
