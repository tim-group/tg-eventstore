package com.timgroup.eventstore.memory;

import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.JavaEventStoreTest;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import org.junit.Test;

import java.time.Clock;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class JavaInMemoryEventStoreTest extends JavaEventStoreTest {

    private final JavaInMemoryEventStore eventStore = new JavaInMemoryEventStore(Clock.systemUTC());

    @Override
    public EventSource eventSource() {
        return new InMemoryEventSource(eventStore);
    }

    private PositionCodec positionCodec() {
        return eventSource().readAll().storePositionCodec();
    }

    @Test
    public void orders_positions_numerically() throws Exception {
        List<Position> positions = Arrays.asList(
                position(10L),
                position(2L),
                position(1L),
                position(100L)
        );
        assertThat(positions.stream().sorted(positionCodec()::comparePositions).collect(toList()), equalTo(Arrays.asList(
                position(1L),
                position(2L),
                position(10L),
                position(100L)
        )));
    }

    @Test
    public void reading_backwards_from_an_empty_position_returns_empty_stream() {
        Stream<ResolvedEvent> resolvedEventStream = eventStore.readAllBackwards(eventStore.emptyStorePosition());

        assertThat(resolvedEventStream.count(), equalTo(0L));
    }

    private Position position(long n) {
        return positionCodec().deserializePosition(Long.toString(n));
    }
}