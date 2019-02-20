package com.timgroup.eventstore.filesystem;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.Objects;

import static java.util.Comparator.comparing;

final class ArchiveDirectoryPosition implements Comparable<ArchiveDirectoryPosition>, Position {
    static final ArchiveDirectoryPosition EMPTY = new ArchiveDirectoryPosition("", ArchivePosition.EMPTY);
    static final Comparator<ArchiveDirectoryPosition> COMPARATOR = comparing(ArchiveDirectoryPosition::getArchive).thenComparing(ArchiveDirectoryPosition::getPosition);
    static final PositionCodec CODEC = PositionCodec.ofComparable(ArchiveDirectoryPosition.class,
            ArchiveDirectoryPosition::deserialise,
            ArchiveDirectoryPosition::serialise
    );

    static ArchiveDirectoryPosition deserialise(String str) {
        if (str.isEmpty()) return EMPTY;
        int splitAt = str.indexOf(':');
        String archive = str.substring(0, splitAt);
        Position position = ArchivePosition.CODEC.deserializePosition(str.substring(splitAt + 1));
        return new ArchiveDirectoryPosition(archive, (ArchivePosition) position);
    }

    static String serialise(ArchiveDirectoryPosition pos) {
        return pos.getArchive().isEmpty() ? "" : pos.getArchive() + ":" + ArchivePosition.CODEC.serializePosition(pos.getPosition());
    }

    @Nonnull
    private final String archive;
    @Nonnull
    private final ArchivePosition position;

    public ArchiveDirectoryPosition(@Nonnull String archive, @Nonnull ArchivePosition position) {
        this.archive = archive;
        this.position = position;
    }

    @Nonnull
    public String getArchive() {
        return archive;
    }

    @Nonnull
    public ArchivePosition getPosition() {
        return position;
    }

    @Override
    public int compareTo(ArchiveDirectoryPosition o) {
        return COMPARATOR.compare(this, o);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ArchiveDirectoryPosition that = (ArchiveDirectoryPosition) o;
        return archive.equals(that.archive) &&
                position.equals(that.position);
    }

    @Override
    public int hashCode() {
        return Objects.hash(archive, position);
    }

    @Override
    public String toString() {
        return archive + ":" + position;
    }
}
