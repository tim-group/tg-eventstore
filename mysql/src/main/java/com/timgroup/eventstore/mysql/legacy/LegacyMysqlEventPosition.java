package com.timgroup.eventstore.mysql.legacy;

import java.util.Comparator;
import java.util.Objects;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;

final class LegacyMysqlEventPosition implements Position {
    final long legacyVersion;

    private LegacyMysqlEventPosition(long legacyVersion) {
        this.legacyVersion = legacyVersion;
    }

    static LegacyMysqlEventPosition fromLegacyVersion(long legacyVersion) {
        return new LegacyMysqlEventPosition(legacyVersion);
    }

    static LegacyMysqlEventPosition fromEventNumber(long eventNumber) {
        return new LegacyMysqlEventPosition(eventNumber + 1L);
    }

    long toEventNumber() {
        return legacyVersion - 1L;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LegacyMysqlEventPosition that = (LegacyMysqlEventPosition) o;
        return legacyVersion == that.legacyVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(legacyVersion);
    }

    @Override
    public String toString() {
        return String.valueOf(legacyVersion);
    }

    static final PositionCodec CODEC = PositionCodec.fromComparator(LegacyMysqlEventPosition.class,
            str -> fromLegacyVersion(Long.parseLong(str)),
            pos -> Long.toString(pos.legacyVersion),
            Comparator.comparingLong(a -> a.legacyVersion));
}
