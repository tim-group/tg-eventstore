package com.timgroup.eventstore.mysql;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;

import javax.annotation.Nullable;

final class BasicMysqlEventStorePosition implements Position, Comparable<BasicMysqlEventStorePosition> {
    static final BasicMysqlEventStorePosition EMPTY_STORE_POSITION = new BasicMysqlEventStorePosition(-1);
    static final PositionCodec CODEC = PositionCodec.ofComparable(BasicMysqlEventStorePosition.class,
            str -> new BasicMysqlEventStorePosition(Long.parseLong(str)),
            pos -> Long.toString(pos.value));
    final long value;

    BasicMysqlEventStorePosition(long value) {
        this.value = value;
    }

    @Override
    public int compareTo(BasicMysqlEventStorePosition o) {
        return Long.compare(value, o.value);
    }

    @Override
    public String toString() {
        return Long.toString(value);
    }

    @Override
    public boolean equals(@Nullable Object o) {
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
