package com.timgroup.eventsubscription.healthcheck;

import com.timgroup.eventstore.api.Position;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@Deprecated
/**
 * @deprecated EventHandler should be used handling the relevant SubscriptionLifecycleEvent
 * */
public interface SubscriptionListener {
    void caughtUpAt(Position position);

    void staleAtVersion(Optional<Position> position);

    void terminated(Position position, Exception e);

    @Deprecated
    /**
     * @deprecated Use EventHandler::onInitialCatchup
     * */
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

    @Deprecated
    /**
     * @deprecated Use EventHandler::onInitialCatchup
     * */
    static SubscriptionListener onInitialCatchup(Runnable callback) {
        return onInitialCatchupAt(ignored -> callback.run());
    }

    @Deprecated
    /**
     * @deprecated Use EventHandler::onTermination
     * */
    static SubscriptionListener onTermination(BiConsumer<? super Position, ? super Throwable> consumer) {
        return new SubscriptionListener() {
            @Override
            public void caughtUpAt(Position position) {
            }

            @Override
            public void staleAtVersion(Optional<Position> position) {
            }

            @Override
            public void terminated(Position position, Exception e) {
                consumer.accept(position, e);
            }
        };
    }
}
