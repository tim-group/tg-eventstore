package com.timgroup.eventstore.api;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

public final class StreamId {
    private final String category;
    private final String id;

    private StreamId(String category, String id) {
        this.category = requireNonNull(category);
        if (category.indexOf('-') != -1) {
            throw new IllegalArgumentException("Event category cannot contain -. Got " + category);
        }
        this.id = requireNonNull(id);
    }

    public static StreamId streamId(String category, String id) {
        return new StreamId(category, id);
    }

    @Nonnull
    public String category() {
        return category;
    }

    @Nonnull
    public String id() {
        return id;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StreamId streamId = (StreamId) o;

        if (!category.equals(streamId.category)) return false;
        return id.equals(streamId.id);

    }

    @Override
    public int hashCode() {
        int result = category.hashCode();
        result = 31 * result + id.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "StreamId{" +
                "category='" + category + '\'' +
                ", id='" + id + '\'' +
                '}';
    }
}
