package com.timgroup.eventstore.stitching;

import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.memory.InMemoryEventSource;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class BackfillParallelChangeCodecTest {

    private final EventSource backfill = new InMemoryEventSource();
    private final EventSource live = new InMemoryEventSource();
    private final String defaultLeftPosition = "10";
    private final EventSource stitchedEventSource = new BackfillStitchingEventSource(backfill, live, backfill.readAll().storePositionCodec().deserializePosition("20"));
    private final PositionCodec stitchedCodec = stitchedEventSource.readAll().storePositionCodec();
    private final PositionCodec unstitchedCodec = live.readAll().storePositionCodec();

    @Test
    public void when_expanding_deserializes_stitched_as_stitched() {
        BackfillParallelChangeCodec codec = new BackfillParallelChangeCodec(stitchedEventSource, defaultLeftPosition, unstitchedCodec);
        assertThat(codec.deserializePosition("10~~~30"), is(stitchedCodec.deserializePosition("10~~~30")));
    }

    @Test
    public void when_expanding_deserializes_unstitched_as_stitched() {
        BackfillParallelChangeCodec codec = new BackfillParallelChangeCodec(stitchedEventSource, defaultLeftPosition, unstitchedCodec);
        assertThat(codec.deserializePosition("30"), is(stitchedCodec.deserializePosition("10~~~30")));
    }

    @Test
    public void when_migrating_deserializes_stitched_as_unstitched() {
        BackfillParallelChangeCodec codec = new BackfillParallelChangeCodec(live, defaultLeftPosition, unstitchedCodec);
        assertThat(codec.deserializePosition("10~~~30"), is(unstitchedCodec.deserializePosition("30")));
    }

    @Test
    public void when_migrating_deserializes_unstitched_as_unstitched() {
        BackfillParallelChangeCodec codec = new BackfillParallelChangeCodec(live, defaultLeftPosition, unstitchedCodec);
        assertThat(codec.deserializePosition("30"), is(unstitchedCodec.deserializePosition("30")));
    }

    @Test
    public void compares_stitched_with_unstitched_with_stitched_event_source() {
        BackfillParallelChangeCodec codec = new BackfillParallelChangeCodec(stitchedEventSource, defaultLeftPosition, unstitchedCodec);
        int compareResult = codec.comparePositions(stitchedCodec.deserializePosition("10~~~30"), unstitchedCodec.deserializePosition("30"));
        assertThat(compareResult, is(0));
    }

    @Test
    public void compares_stitched_with_unstitched_with_unstitched_event_source() {
        BackfillParallelChangeCodec codec = new BackfillParallelChangeCodec(live, defaultLeftPosition, unstitchedCodec);
        int compareResult = codec.comparePositions(stitchedCodec.deserializePosition("10~~~30"), unstitchedCodec.deserializePosition("30"));
        assertThat(compareResult, is(0));
    }
}