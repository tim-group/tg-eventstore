package com.timgroup.eventsubscription;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.timgroup.eventstore.api.Position;

import java.util.ArrayList;
import java.util.List;

final class SequencingEventHandler implements EventHandler {
    static SequencingEventHandler flatten(List<? extends EventHandler> handlers) {
        List<EventHandler> input = new ArrayList<>(handlers);
        List<EventHandler> output = new ArrayList<>();
        while (!input.isEmpty()) {
            EventHandler handler = input.remove(0);
            if (handler instanceof SequencingEventHandler) {
                input.addAll(0, ((SequencingEventHandler) handler).handlers);
            }
            else {
                output.add(handler);
            }
        }
        return new SequencingEventHandler(output);
    }

    private final List<EventHandler> handlers;

    SequencingEventHandler(List<? extends EventHandler> handlers) {
        this.handlers = ImmutableList.copyOf(handlers);
    }

    @Override
    public void apply(Position position, Event deserialized) {
        for (EventHandler handler : handlers) {
            handler.apply(position, deserialized);
        }
    }

    @VisibleForTesting
    int size() {
        return handlers.size();
    }
}
