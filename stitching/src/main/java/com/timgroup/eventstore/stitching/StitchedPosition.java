package com.timgroup.eventstore.stitching;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;

import java.util.regex.Pattern;

final class StitchedPosition implements Position {
    final Position backfillPosition;
    final Position livePosition;

    StitchedPosition(Position backfillPosition, Position livePosition) {
        this.backfillPosition = backfillPosition;
        this.livePosition = livePosition;
    }

    public boolean isInBackfill(StitchedPosition emptyStorePosition) {
        return this.livePosition.equals(emptyStorePosition.livePosition);
    }

    static final class StitchedPositionCodec implements PositionCodec {
        private static final String STITCH_SEPARATOR = "~~~";
        private static final Pattern STITCH_PATTERN = Pattern.compile(Pattern.quote(STITCH_SEPARATOR));

        private final PositionCodec backfillPositionCodec;
        private final PositionCodec livePositionCodec;

        StitchedPositionCodec(PositionCodec backfillPositionCodec, PositionCodec livePositionCodec) {
            this.backfillPositionCodec = backfillPositionCodec;
            this.livePositionCodec = livePositionCodec;
        }

        @Override
        public Position deserializePosition(String string) {
            String[] positions = STITCH_PATTERN.split(string, 2);
            return new StitchedPosition(
                    backfillPositionCodec.deserializePosition(positions[0]),
                    livePositionCodec.deserializePosition(positions[1])
            );
        }

        @Override
        public String serializePosition(Position position) {
            StitchedPosition stitchedPosition = (StitchedPosition) position;
            return backfillPositionCodec.serializePosition(stitchedPosition.backfillPosition)
                    + STITCH_SEPARATOR
                    + livePositionCodec.serializePosition(stitchedPosition.livePosition);
        }
    }
}
