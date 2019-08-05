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
    private final String backfillEndPosition = "10";
    private final String liveStartCutoffPosition = "20";
    private final EventSource stitchedEventSource = new BackfillStitchingEventSource(backfill, live, backfill.readAll().storePositionCodec().deserializePosition(liveStartCutoffPosition));
    private final PositionCodec stitchedCodec = stitchedEventSource.readAll().storePositionCodec();
    private final PositionCodec unstitchedCodec = live.readAll().storePositionCodec();

    @Test
    public void when_expanding_deserializes_stitched_as_stitched() {
        BackfillParallelChangeCodec codec = new BackfillParallelChangeCodec(stitchedEventSource, backfillEndPosition, liveStartCutoffPosition);
        assertThat(codec.deserializePosition("10~~~30"), is(stitchedCodec.deserializePosition("10~~~30")));
    }

    @Test
    public void when_expanding_deserializes_unstitched_as_stitched() {
        BackfillParallelChangeCodec codec = new BackfillParallelChangeCodec(stitchedEventSource, backfillEndPosition, liveStartCutoffPosition);
        assertThat(codec.deserializePosition("30"), is(stitchedCodec.deserializePosition("10~~~30")));
    }

    @Test
    public void when_migrating_deserializes_stitched_as_unstitched() {
        BackfillParallelChangeCodec codec = new BackfillParallelChangeCodec(live, backfillEndPosition, liveStartCutoffPosition);
        assertThat(codec.deserializePosition("10~~~30"), is(unstitchedCodec.deserializePosition("30")));
    }

    @Test
    public void when_migrating_deserializes_unstitched_as_unstitched() {
        BackfillParallelChangeCodec codec = new BackfillParallelChangeCodec(live, backfillEndPosition, liveStartCutoffPosition);
        assertThat(codec.deserializePosition("30"), is(unstitchedCodec.deserializePosition("30")));
    }

    @Test
    public void compares_stitched_with_unstitched_with_stitched_event_source() {
        BackfillParallelChangeCodec codec = new BackfillParallelChangeCodec(stitchedEventSource, backfillEndPosition, liveStartCutoffPosition);
        int compareResult = codec.comparePositions(stitchedCodec.deserializePosition("10~~~30"), unstitchedCodec.deserializePosition("30"));
        assertThat(compareResult, is(0));
    }

    @Test
    public void compares_stitched_with_unstitched_with_unstitched_event_source() {
        BackfillParallelChangeCodec codec = new BackfillParallelChangeCodec(live, backfillEndPosition, liveStartCutoffPosition);
        int compareResult = codec.comparePositions(stitchedCodec.deserializePosition("10~~~30"), unstitchedCodec.deserializePosition("30"));
        assertThat(compareResult, is(0));
    }

    @Test
    public void throws_and_exception_if_attempt_is_made_to_compare_backfill_position() {
        BackfillParallelChangeCodec codec = new BackfillParallelChangeCodec(live, backfillEndPosition, liveStartCutoffPosition);
        try {
            codec.comparePositions(stitchedCodec.deserializePosition("10~~~20"), unstitchedCodec.deserializePosition("30"));
            fail("Expected exception not thrown");
        } catch (IllegalStateException e ) {
            assertThat(e.getMessage(), is("Cannot handle positions in the backfill"));
        }
    }

    @Test
    public void throws_and_exception_if_attempt_is_made_to_deserialise_backfill_position() {
        BackfillParallelChangeCodec codec = new BackfillParallelChangeCodec(live, backfillEndPosition, liveStartCutoffPosition);
        try {
            codec.deserializePosition("10~~~20");
            fail("Expected exception not thrown");
        } catch (IllegalStateException e ) {
            assertThat(e.getMessage(), is("Cannot handle positions in the backfill"));
        }
    }
}