package com.timgroup.eventstore.stitching;

import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;

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
    private final Position backfillEndPosition;
    private final Position liveCutoffStartPosition;
    private final boolean underlyingIsStitched;
    private final PositionCodec livePositionCodec;

    public BackfillParallelChangeCodec(EventSource eventSource, String backfillEndPosition, String liveCutoffStartPosition) {
        this.underlying = eventSource.readAll().storePositionCodec();
        if (eventSource instanceof BackfillStitchingEventSource) {
            underlyingIsStitched = true;
            livePositionCodec = ((BackfillStitchingEventSource) eventSource).live.readAll().storePositionCodec();
            this.backfillEndPosition = ((BackfillStitchingEventSource) eventSource).backfill.readAll().storePositionCodec().deserializePosition(backfillEndPosition);
        } else {
            underlyingIsStitched = false;
            livePositionCodec = eventSource.readAll().storePositionCodec();
            this.backfillEndPosition = null;
        }
        this.liveCutoffStartPosition = livePositionCodec.deserializePosition(liveCutoffStartPosition);
    }

    @Override
    public Position deserializePosition(String position) {
        String[] positions = StitchedPosition.STITCH_PATTERN.split(position);
        if (isStitchedPosition(positions)) {
            Position livePosition = livePositionCodec.deserializePosition(positions[1]);
            requireNotInBackfill(livePosition);
            if (underlyingIsStitched) {
                return underlying.deserializePosition(position);
            } else {
                return livePosition;
            }
        } else {
            Position livePosition = livePositionCodec.deserializePosition(position);
            requireNotInBackfill(livePosition);
            if (underlyingIsStitched) {
                return new StitchedPosition(backfillEndPosition, livePosition);
            } else {
                return livePosition;
            }
        }
    }

    @Override
    public String serializePosition(Position position) {
        return underlying.serializePosition(position);
    }

    @Override
    public int comparePositions(Position left, Position right) {
        return livePositionCodec.comparePositions(requireNotInBackfill(getLivePosition(left)), requireNotInBackfill(getLivePosition(right)));
    }

    private boolean isStitchedPosition(String[] positions) {
        if (positions.length == 1) {
            return false;
        } else if (positions.length == 2) {
            return true;
        } else {
            throw new IllegalArgumentException("Cannot handle nested stitch separators for position " + String.join(STITCH_SEPARATOR, positions));
        }
    }

    private Position requireNotInBackfill(Position livePosition) {
        if (livePositionCodec.comparePositions(livePosition, liveCutoffStartPosition) < 1) {
            throw new IllegalStateException("Cannot handle positions in the backfill");
        }
        return livePosition;
    }

    private Position getLivePosition(Position position) {
        if (position instanceof StitchedPosition) {
            return ((StitchedPosition) position).livePosition;
        } else {
            return position;
        }
    }
}
