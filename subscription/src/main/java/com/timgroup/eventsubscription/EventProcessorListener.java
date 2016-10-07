package com.timgroup.eventsubscription;

public interface EventProcessorListener {
    void eventProcessingFailed(long version, Exception e);

    void eventProcessed(long version);

    void eventDeserializationFailed(long version, Exception e);

    void eventDeserialized(long version);
}
