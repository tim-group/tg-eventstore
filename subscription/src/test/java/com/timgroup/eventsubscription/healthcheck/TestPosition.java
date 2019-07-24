package com.timgroup.eventsubscription.healthcheck;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;

import java.util.Comparator;

final class TestPosition implements Position {
    static final PositionCodec CODEC = PositionCodec.fromComparator(TestPosition.class,
            string -> new TestPosition(Long.parseLong(string)),
            position -> Long.toString(position.value),
            Comparator.comparingLong(position -> position.value)
    );

    private final long value;

    TestPosition(long value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TestPosition)) return false;

        TestPosition that = (TestPosition) o;

        return value == that.value;
    }

    @Override
    public int hashCode() {
        return (int) (value ^ (value >>> 32));
    }

    @Override
    public String toString() {
        return Long.toString(value);
    }
}
