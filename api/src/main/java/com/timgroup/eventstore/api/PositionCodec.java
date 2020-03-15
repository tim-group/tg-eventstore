package com.timgroup.eventstore.api;

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.function.Function;

public interface PositionCodec {
    @Nonnull
    Position deserializePosition(String string);

    @Nonnull
    String serializePosition(Position position);

    default int comparePositions(Position left, Position right) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    static <T extends Position> PositionCodec fromComparator(
            Class<T> clazz,
            Function<? super String, ? extends T> deserialize,
            Function<? super T, String> serialize,
            Comparator<? super T> comparator) {
        return new PositionCodec() {
            @Nonnull
            @Override
            public Position deserializePosition(String string) {
                return deserialize.apply(string);
            }

            @Nonnull
            @Override
            public String serializePosition(Position position) {
                return serialize.apply(clazz.cast(position));
            }

            @Override
            public int comparePositions(Position left, Position right) {
                return comparator.compare(clazz.cast(left), clazz.cast(right));
            }

            @Override
            public String toString() {
                return "PositionCodec{" + clazz.getName() + ", " + comparator + "}";
            }
        };
    }

    @Nonnull
    static <T extends Position & Comparable<T>> PositionCodec ofComparable(
            Class<T> clazz,
            Function<? super String, ? extends T> deserialize,
            Function<? super T, String> serialize) {
        return new PositionCodec() {
            @Nonnull
            @Override
            public Position deserializePosition(String string) {
                return deserialize.apply(string);
            }

            @Nonnull
            @Override
            public String serializePosition(Position position) {
                return serialize.apply(clazz.cast(position));
            }

            @Override
            public int comparePositions(Position left, Position right) {
                return clazz.cast(left).compareTo(clazz.cast(right));
            }

            @Override
            public String toString() {
                return "PositionCodec{" + clazz.getName() + "}";
            }
        };
    }
}
