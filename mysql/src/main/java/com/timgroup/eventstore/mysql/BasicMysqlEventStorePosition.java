package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.Position;

final class BasicMysqlEventStorePosition implements Position {
    static final BasicMysqlEventStorePosition EMPTY_STORE_POSITION = new BasicMysqlEventStorePosition(-1);
    final long value;

    BasicMysqlEventStorePosition(long value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return Long.toString(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BasicMysqlEventStorePosition)) return false;

        BasicMysqlEventStorePosition that = (BasicMysqlEventStorePosition) o;

        return value == that.value;

    }

    @Override
    public int hashCode() {
        return (int) (value ^ (value >>> 32));
    }
}
