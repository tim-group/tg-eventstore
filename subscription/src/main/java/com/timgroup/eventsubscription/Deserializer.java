package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.EventInStream;

public interface Deserializer<T> {
    T deserialize(EventInStream event);
}
