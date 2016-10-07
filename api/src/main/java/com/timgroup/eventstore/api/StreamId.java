package com.timgroup.eventstore.api;

public final class StreamId {
    private final String category;
    private final String id;

    private StreamId(String category, String id) {
        this.category = category;
        this.id = id;
    }

    public static StreamId streamId(String category, String id) {
        return new StreamId(category, id);
    }

    public String category() {
        return category;
    }

    public String id() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
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
