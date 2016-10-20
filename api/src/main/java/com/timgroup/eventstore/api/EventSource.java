package com.timgroup.eventstore.api;

public interface EventSource {
    EventReader readAll();
    EventCategoryReader readCategory();
    EventStreamReader readStream();
    EventStreamWriter writeStream();
    PositionCodec positionCodec();
}
