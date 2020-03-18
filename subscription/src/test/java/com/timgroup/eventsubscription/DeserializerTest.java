package com.timgroup.eventsubscription;

import com.google.common.collect.ImmutableList;
import com.timgroup.eventstore.api.EventRecord;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class DeserializerTest {

    private static EventRecord eventOfType(String eventType) {
        return eventRecord(Instant.EPOCH, streamId("any", "any"), 0L, eventType, new byte[0], new byte[0]);
    }

    @Test
    public void applying() {
        Deserializer<String> deserializer = Deserializer.applying(EventRecord::eventType);

        List<String> outputs = new ArrayList<>();
        deserializer.deserialize(eventOfType("SomeEvent"), outputs::add);

        assertThat(outputs, equalTo(ImmutableList.of("SomeEvent")));
    }

    @Test
    public void applyingOptional() {
        Deserializer<String> deserializer = Deserializer.applyingOptional(event -> Optional.ofNullable(event.eventType().endsWith("Ignore") ? null : event.eventType()));

        List<String> outputs = new ArrayList<>();
        deserializer.deserialize(eventOfType("SomeEvent"), outputs::add);
        deserializer.deserialize(eventOfType("SomeEventIgnore"), outputs::add);

        assertThat(outputs, equalTo(ImmutableList.of("SomeEvent")));
    }

    @Test
    public void filtering() {
        Deserializer<String> deserializer = Deserializer.filtering(event -> !event.eventType().endsWith("Ignore"), Deserializer.applying(EventRecord::eventType));

        List<String> outputs = new ArrayList<>();
        deserializer.deserialize(eventOfType("SomeEvent"), outputs::add);
        deserializer.deserialize(eventOfType("SomeEventIgnore"), outputs::add);

        assertThat(outputs, equalTo(ImmutableList.of("SomeEvent")));
    }

    @Test
    public void filtering_using_default_method() {
        Deserializer<String> deserializer = Deserializer.applying(EventRecord::eventType).filter(event -> !event.eventType().endsWith("Ignore"));

        List<String> outputs = new ArrayList<>();
        deserializer.deserialize(eventOfType("SomeEvent"), outputs::add);
        deserializer.deserialize(eventOfType("SomeEventIgnore"), outputs::add);

        assertThat(outputs, equalTo(ImmutableList.of("SomeEvent")));
    }

    @Test
    public void wrapping() {
        Deserializer<String> deserializer = Deserializer.wrapping((type, event) -> type + "=" + event.eventType(), Deserializer.applying(EventRecord::eventType));

        List<String> outputs = new ArrayList<>();
        deserializer.deserialize(eventOfType("SomeEvent"), outputs::add);

        assertThat(outputs, equalTo(ImmutableList.of("SomeEvent=SomeEvent")));
    }

    @Test
    public void wrapping_using_default_method() {
        Deserializer<String> deserializer = Deserializer.applying(EventRecord::eventType).map((type, event) -> type + "=" + event.eventType());

        List<String> outputs = new ArrayList<>();
        deserializer.deserialize(eventOfType("SomeEvent"), outputs::add);

        assertThat(outputs, equalTo(ImmutableList.of("SomeEvent=SomeEvent")));
    }
}
