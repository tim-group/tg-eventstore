package com.timgroup.eventstore.mysql.legacy;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;

import java.util.Objects;

public final class LegacyMysqlEventPosition implements Position {
    public final long legacyVersion;

    private LegacyMysqlEventPosition(long legacyVersion) {
        this.legacyVersion = legacyVersion;
    }

    public static LegacyMysqlEventPosition fromLegacyVersion(long legacyVersion) {
        return new LegacyMysqlEventPosition(legacyVersion);
    }

    public static LegacyMysqlEventPosition fromEventNumber(long eventNumber) {
        return new LegacyMysqlEventPosition(eventNumber + 1);
    }

    public long toEventNumber() {
        return legacyVersion - 1;
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
        return "LegacyMysqlEventPosition{" +
                "legacyVersion=" + legacyVersion +
                '}';
    }

    public static final class LegacyPositionCodec implements PositionCodec {
        @Override
        public Position deserializePosition(String serialisedPosition) {
            return fromLegacyVersion(Long.parseLong(serialisedPosition));
        }

        @Override
        public String serializePosition(Position position) {
            return Long.toString(((LegacyMysqlEventPosition) position).legacyVersion);
        }
    }
}
