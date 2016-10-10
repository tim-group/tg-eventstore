package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.EventRecord;

public interface Deserializer<T> {
    T deserialize(EventRecord event);
}
