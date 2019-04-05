package com.timgroup.eventsubscription;

import com.google.common.collect.ImmutableList;
import com.timgroup.eventstore.api.Position;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

public class SequencingEventHandlerTest {
    private static final Position A_POSITION = new Position(){};
    private static final Event AN_EVENT = new Event(){};

    @Test
    public void invokes_handlers_in_order() {
        EventHandler h1 = mock(EventHandler.class);
        EventHandler h2 = mock(EventHandler.class);

        SequencingEventHandler sequencer = new SequencingEventHandler(ImmutableList.of(h1, h2));
        sequencer.apply(A_POSITION, AN_EVENT);

        assertThat(sequencer, sequenceWithSize(2));

        InOrder inOrder = Mockito.inOrder(h1, h2);
        inOrder.verify(h1).apply(A_POSITION, AN_EVENT);
        inOrder.verify(h2).apply(A_POSITION, AN_EVENT);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void preserves_order_in_flattened_sequence() {
        EventHandler h1 = mock(EventHandler.class);
        EventHandler h2 = mock(EventHandler.class);
        EventHandler h3 = mock(EventHandler.class);
        EventHandler h4 = mock(EventHandler.class);

        SequencingEventHandler sequencer = SequencingEventHandler.flatten(ImmutableList.of(
                new SequencingEventHandler(ImmutableList.of(h1, h2)),
                new SequencingEventHandler(ImmutableList.of(h3, h4))
        ));
        sequencer.apply(A_POSITION, AN_EVENT);

        assertThat(sequencer, sequenceWithSize(4));

        InOrder inOrder = Mockito.inOrder(h1, h2, h3, h4);
        inOrder.verify(h1).apply(A_POSITION, AN_EVENT);
        inOrder.verify(h2).apply(A_POSITION, AN_EVENT);
        inOrder.verify(h3).apply(A_POSITION, AN_EVENT);
        inOrder.verify(h4).apply(A_POSITION, AN_EVENT);
        inOrder.verifyNoMoreInteractions();
    }

    static Matcher<EventHandler> sequenceWithSize(int size) {
        return new TypeSafeDiagnosingMatcher<EventHandler>() {
            @Override
            protected boolean matchesSafely(EventHandler item, Description mismatchDescription) {
                if (!(item instanceof SequencingEventHandler)) {
                    mismatchDescription.appendText("not a sequence");
                    return false;
                }
                SequencingEventHandler sequencer = (SequencingEventHandler) item;
                mismatchDescription.appendText("size was ").appendValue(sequencer.size());
                return sequencer.size() == size;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("sequence with size ").appendValue(size);
            }
        };
    }
}