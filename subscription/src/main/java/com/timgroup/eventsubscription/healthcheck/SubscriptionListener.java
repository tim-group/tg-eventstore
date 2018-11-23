package com.timgroup.eventsubscription.healthcheck;

import com.timgroup.eventstore.api.Position;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public interface SubscriptionListener {
    void caughtUpAt(Position position);

    void staleAtVersion(Optional<Position> position);

    void terminated(Position position, Exception e);

    static SubscriptionListener onInitialCatchupAt(Consumer<? super Position> callback) {
        return new SubscriptionListener() {
            private final AtomicBoolean seenCatchup = new AtomicBoolean();

            @Override
            public void caughtUpAt(@Nonnull Position position) {
                if (seenCatchup.compareAndSet(false, true)) {
                    callback.accept(position);
                }
            }

            @Override
            public void staleAtVersion(@Nonnull Optional<Position> position) {
            }

            @Override
            public void terminated(@Nonnull Position position, @Nonnull Exception e) {
            }
        };
    }

    static SubscriptionListener onInitialCatchup(Runnable callback) {
        return onInitialCatchupAt(ignored -> callback.run());
    }
}
