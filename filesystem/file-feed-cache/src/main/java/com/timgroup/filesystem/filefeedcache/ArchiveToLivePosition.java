package com.timgroup.filesystem.filefeedcache;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.archiver.S3ArchivePosition;

import java.util.Comparator;
import java.util.Objects;

public final class ArchiveToLivePosition implements Position {
    static PositionCodec codec(PositionCodec archiveCodec, PositionCodec liveCodec, Position lastArchivePosition) {
        return PositionCodec.fromComparator(ArchiveToLivePosition.class,
                string -> new ArchiveToLivePosition(
                        archiveCodec.comparePositions(archiveCodec.deserializePosition(string), lastArchivePosition) <= 0
                            ? archiveCodec.deserializePosition(string)
                            : liveCodec.deserializePosition(string)
                ),
                pos -> serialise(pos, archiveCodec, liveCodec),
                Comparator.comparing(
                        pos -> Long.parseLong(serialise(pos, archiveCodec, liveCodec)),
                        Comparator.comparingLong(pos -> pos)
                ));
    }

    private static String serialise(ArchiveToLivePosition pos, PositionCodec archiveCodec, PositionCodec liveCodec) {
        return pos.isArchivePosition() ? archiveCodec.serializePosition(pos.underlying) : liveCodec.serializePosition(pos.underlying);
    }

    public final Position underlying;

    ArchiveToLivePosition(Position underlying) {
        this.underlying = underlying;
    }

    public boolean isArchivePosition() {
        return underlying instanceof S3ArchivePosition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ArchiveToLivePosition that = (ArchiveToLivePosition) o;
        return underlying.equals(that.underlying);
    }

    @Override
    public int hashCode() {
        return Objects.hash(underlying);
    }

    @Override
    public String toString() {
        return "ArchiveToLivePosition{" +
                "underlying=" + underlying +
                '}';
    }
}
