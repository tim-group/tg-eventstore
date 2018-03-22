package com.timgroup.eventstore.api;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Comparator;
import java.util.function.Function;

@ParametersAreNonnullByDefault
public interface PositionCodec {
    Position deserializePosition(String string);

    String serializePosition(Position position);

    default int comparePositions(Position left, Position right) {
        throw new UnsupportedOperationException();
    }

    static <T extends Position> PositionCodec fromComparator(
            Class<T> clazz,
            Function<String, ? extends T> deserialize,
            Function<? super T, String> serialize,
            Comparator<T> comparator) {
        return new PositionCodec() {
            @Override
            public Position deserializePosition(String string) {
                return deserialize.apply(string);
            }

            @Override
            public String serializePosition(Position position) {
                return serialize.apply(clazz.cast(position));
            }

            @Override
            public int comparePositions(Position left, Position right) {
                return comparator.compare(clazz.cast(left), clazz.cast(right));
            }
        };
    }

    static <T extends Position & Comparable<T>> PositionCodec ofComparable(
            Class<T> clazz,
            Function<String, ? extends T> deserialize,
            Function<? super T, String> serialize) {
        return new PositionCodec() {
            @Override
            public Position deserializePosition(String string) {
                return deserialize.apply(string);
            }

            @Override
            public String serializePosition(Position position) {
                return serialize.apply(clazz.cast(position));
            }

            @Override
            public int comparePositions(Position left, Position right) {
                return clazz.cast(left).compareTo(clazz.cast(right));
            }
        };
    }
}
