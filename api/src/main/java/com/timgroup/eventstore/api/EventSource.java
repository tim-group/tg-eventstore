package com.timgroup.eventstore.api;

import com.timgroup.tucker.info.Component;

import java.util.Collection;

public interface EventSource {
    EventReader readAll();
    EventCategoryReader readCategory();
    EventStreamReader readStream();
    EventStreamWriter writeStream();
    PositionCodec positionCodec();
    Collection<Component> monitoring();
}
