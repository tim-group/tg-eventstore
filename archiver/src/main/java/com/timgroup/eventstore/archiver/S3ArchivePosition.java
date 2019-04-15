package com.timgroup.eventstore.archiver;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;

import javax.annotation.Nullable;

public class S3ArchivePosition implements Position, Comparable<S3ArchivePosition> {
    static final S3ArchivePosition EMPTY_STORE_POSITION = new S3ArchivePosition(-1);
    static final PositionCodec CODEC = PositionCodec.ofComparable(S3ArchivePosition.class,
            str -> new S3ArchivePosition(Long.parseLong(str)),
            pos -> Long.toString(pos.value));

    public final long value;

    S3ArchivePosition(long value) {
        this.value = value;
    }

    @Override
    public int compareTo(S3ArchivePosition o) {
        return Long.compare(value, o.value);
    }

    @Override
    public String toString() {
        return Long.toString(value);
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (!(o instanceof S3ArchivePosition)) return false;

        S3ArchivePosition that = (S3ArchivePosition) o;

        return value == that.value;
    }

    @Override
    public int hashCode() {
        return (int) (value ^ (value >>> 32));
    }
}
