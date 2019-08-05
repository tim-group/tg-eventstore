package com.timgroup.eventstore.stitching;

import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;

import java.util.Objects;

import static com.timgroup.eventstore.stitching.StitchedPosition.STITCH_SEPARATOR;

/**
 * Codec for use in three phase Expand, Migrate, Contract changes to replace a {@link com.timgroup.eventstore.stitching.BackfillStitchingEventSource} with a non stitched event source
 * <p>
 * In the expand phase provide a stitched event source. This will read stitched or unstitched positions and write stitch positions.
 * <p>
 * In the migrate phase provide an unstitched event source. This will read stitched or unstitched positions and write unstitched positions.
 * <p>
 * In the contract phase remove this codec and use the unstitched event sources codec directly.
 */
public class BackfillParallelChangeCodec implements PositionCodec {

    private final PositionCodec underlying;
    private final String defaultLeftPosition;
    private final boolean underlyingIsStitched;
    private final PositionCodec livePositionCodec;

    public BackfillParallelChangeCodec(EventSource eventSource, String defaultLeftPosition, PositionCodec livePositionCodec) {
        this.underlying = eventSource.readAll().storePositionCodec();
        this.defaultLeftPosition = Objects.requireNonNull(defaultLeftPosition);
        this.livePositionCodec = livePositionCodec;
        underlyingIsStitched = eventSource instanceof BackfillStitchingEventSource;
    }

    @Override
    public Position deserializePosition(String position) {
        String[] positions = StitchedPosition.STITCH_PATTERN.split(position);
        switch (positions.length) {
            case 1:
                if (underlyingIsStitched) {
                    return underlying.deserializePosition(defaultLeftPosition + STITCH_SEPARATOR + position);
                } else {
                    return underlying.deserializePosition(position);
                }
            case 2:
                if (underlyingIsStitched) {
                    return underlying.deserializePosition(position);
                } else {
                    return underlying.deserializePosition(positions[1]);
                }
            default:
                throw new IllegalArgumentException("Cannot handle nested stitch separators for position " + position);
        }
    }

    @Override
    public String serializePosition(Position position) {
        return underlying.serializePosition(position);
    }

    @Override
    public int comparePositions(Position left, Position right) {
        return livePositionCodec.comparePositions(getLivePosition(left), getLivePosition(right));
    }

    private Position getLivePosition(Position position) {
        if (position instanceof StitchedPosition) {
            return ((StitchedPosition) position).livePosition;
        } else {
            return position;
        }
    }
}
