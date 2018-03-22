package com.timgroup.eventstore.api;

import com.timgroup.tucker.info.Component;

import javax.annotation.Nonnull;
import java.util.Collection;

public interface EventSource {
    @Nonnull EventReader readAll();
    @Nonnull EventCategoryReader readCategory();
    @Nonnull EventStreamReader readStream();
    @Nonnull EventStreamWriter writeStream();
    @Nonnull PositionCodec positionCodec();
    @Nonnull Collection<Component> monitoring();
}
