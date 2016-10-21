package com.timgroup.eventstore.api;

import org.junit.Test;

public class StreamIdTest {
    @Test(expected = IllegalArgumentException.class) public void
    cannot_use_dash_in_category() {
        StreamId.streamId("abc-def", "id");
    }
}