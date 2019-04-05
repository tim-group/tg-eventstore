package com.timgroup.eventsubscription;

import com.timgroup.eventstore.api.Position;
import com.youdevise.testutils.matchers.Contains;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.timgroup.eventsubscription.SequencingEventHandlerTest.sequenceWithSize;
import static org.hamcrest.MatcherAssert.assertThat;

public class EventHandlerTest {
    private static final Position A_POSITION = new Position(){};
    private static final Event AN_EVENT = new Event(){};

    @Test
    public void andThen_produces_a_flattened_concatenating_handler() {
        List<Event> events1 = new ArrayList<>();
        List<Event> events2 = new ArrayList<>();
        List<Event> events3 = new ArrayList<>();
        EventHandler h1 = EventHandler.ofConsumer(events1::add);
        EventHandler h2 = EventHandler.ofConsumer(events2::add);
        EventHandler h3 = EventHandler.ofConsumer(events3::add);

        EventHandler eventHandler = h1.andThen(h2).andThen(h3);
        assertThat(eventHandler, sequenceWithSize(3));
        eventHandler.apply(A_POSITION, AN_EVENT);

        assertThat(events1, Contains.only(AN_EVENT));
        assertThat(events2, Contains.only(AN_EVENT));
        assertThat(events3, Contains.only(AN_EVENT));
    }
}
