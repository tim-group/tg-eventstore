package com.timgroup.eventstore.api;

import com.timgroup.tucker.info.Component;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Objects.requireNonNull;

public interface EventSource {
    @Nonnull EventReader readAll();
    @Nonnull EventCategoryReader readCategory();
    @Nonnull EventStreamReader readStream();
    @Nonnull EventStreamWriter writeStream();
    /**
     * @deprecated use positionCodec() lower down on EventReader et al
     */
    @Deprecated
    @Nonnull PositionCodec positionCodec();
    @Nonnull Collection<Component> monitoring();

    default EventSource withMonitoring(Collection<Component> moreMonitoring) {
        requireNonNull(moreMonitoring);

        return new EventSource() {
            @Nonnull
            @Override
            public EventReader readAll() {
                return EventSource.this.readAll();
            }

            @Nonnull
            @Override
            public EventCategoryReader readCategory() {
                return EventSource.this.readCategory();
            }

            @Nonnull
            @Override
            public EventStreamReader readStream() {
                return EventSource.this.readStream();
            }

            @Nonnull
            @Override
            public EventStreamWriter writeStream() {
                return EventSource.this.writeStream();
            }

            @Nonnull
            @Override
            public PositionCodec positionCodec() {
                return EventSource.this.positionCodec();
            }

            @Nonnull
            @Override
            public Collection<Component> monitoring() {
                List<Component> combinedMonitoring = new ArrayList<>();
                combinedMonitoring.addAll(EventSource.this.monitoring());
                combinedMonitoring.addAll(moreMonitoring);
                return combinedMonitoring;
            }

            @Override
            public String toString() {
                return super.toString();
            }
        };
    }
}
