package com.timgroup.filesystem.filefeedcache;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.archiver.S3ArchivePosition;

import java.util.Comparator;
import java.util.Objects;

public class TransitionPosition implements Position {
    static PositionCodec codec(PositionCodec archiveCodec, PositionCodec liveCodec, long cutover) {
        return PositionCodec.fromComparator(TransitionPosition.class,
                string -> new TransitionPosition(Long.valueOf(string) > cutover ? liveCodec.deserializePosition(string) : archiveCodec.deserializePosition(string)),
                stitchedPosition -> (stitchedPosition.position instanceof S3ArchivePosition) ? archiveCodec.serializePosition(stitchedPosition) : liveCodec.serializePosition(stitchedPosition),
                Comparator.comparing((TransitionPosition p) -> p.position, archiveCodec::comparePositions).thenComparing(p -> p.position, liveCodec::comparePositions));
    }

    public final Position position;

    TransitionPosition(Position appendPosition) {
        this.position = appendPosition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransitionPosition that = (TransitionPosition) o;
        return position == that.position;
    }

    @Override
    public int hashCode() {
        return Objects.hash(position);
    }

    @Override
    public String toString() {
        return "AppendedPosition{" +
                "position=" + position +
                '}';
    }
}
