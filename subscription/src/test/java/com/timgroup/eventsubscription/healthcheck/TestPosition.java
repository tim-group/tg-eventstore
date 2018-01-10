package com.timgroup.eventsubscription.healthcheck;

import com.timgroup.eventstore.api.Position;

class TestPosition implements Position {
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
