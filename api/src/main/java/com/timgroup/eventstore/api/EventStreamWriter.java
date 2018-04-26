package com.timgroup.eventstore.api;

import java.util.Collection;
import java.util.OptionalLong;

public interface EventStreamWriter {
    void write(StreamId streamId, Collection<NewEvent> events);

    void write(StreamId streamId, Collection<NewEvent> events, long expectedVersion);

    default void execute(Collection<StreamWriteRequest> writeRequests) {
        writeRequests.forEach(request -> {
            if (request.expectedVersion.isPresent()) {
                write(request.streamId, request.events, request.expectedVersion.getAsLong());
            } else {
                write(request.streamId, request.events);
            }
        });
    }

    class StreamWriteRequest {
        public final StreamId streamId;
        public final OptionalLong expectedVersion;
        public final Collection<NewEvent> events;

        public StreamWriteRequest(StreamId streamId, OptionalLong expectedVersion, Collection<NewEvent> events) {
            this.streamId = streamId;
            this.expectedVersion = expectedVersion;
            this.events = events;
        }

        @Override
        public String toString() {
            return "StreamWriteRequest{" +
                    "streamId=" + streamId +
                    ", expectedVersion=" + expectedVersion +
                    ", events=" + events +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            StreamWriteRequest request = (StreamWriteRequest) o;

            if (streamId != null ? !streamId.equals(request.streamId) : request.streamId != null) return false;
            if (expectedVersion != null ? !expectedVersion.equals(request.expectedVersion) : request.expectedVersion != null)
                return false;
            return events != null ? events.equals(request.events) : request.events == null;
        }

        @Override
        public int hashCode() {
            int result = streamId != null ? streamId.hashCode() : 0;
            result = 31 * result + (expectedVersion != null ? expectedVersion.hashCode() : 0);
            result = 31 * result + (events != null ? events.hashCode() : 0);
            return result;
        }
    }
}
