package com.timgroup.eventstore.stitching;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.regex.Pattern;

import static java.util.Comparator.comparing;

final class StitchedPosition implements Position {
    static final String STITCH_SEPARATOR = "~~~";
    static final Pattern STITCH_PATTERN = Pattern.compile(Pattern.quote(STITCH_SEPARATOR));
    static PositionCodec codec(PositionCodec backfillCodec, PositionCodec liveCodec) {
        return PositionCodec.fromComparator(StitchedPosition.class,
                string -> {
                    String[] positions = STITCH_PATTERN.split(string, 2);
                    return new StitchedPosition(
                            backfillCodec.deserializePosition(positions[0]),
                            liveCodec.deserializePosition(positions[1])
                    );
                },
                stitchedPosition -> {
                    return backfillCodec.serializePosition(stitchedPosition.backfillPosition)
                            + STITCH_SEPARATOR
                            + liveCodec.serializePosition(stitchedPosition.livePosition);
                },
                comparing((StitchedPosition p) -> p.backfillPosition, backfillCodec::comparePositions)
                        .thenComparing(p -> p.livePosition, liveCodec::comparePositions));
    }

    final Position backfillPosition;
    final Position livePosition;

    StitchedPosition(Position backfillPosition, Position livePosition) {
        this.backfillPosition = backfillPosition;
        this.livePosition = livePosition;
    }

    public boolean isInBackfill(StitchedPosition emptyStorePosition) {
        return this.livePosition.equals(emptyStorePosition.livePosition);
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StitchedPosition that = (StitchedPosition) o;
        return Objects.equals(backfillPosition, that.backfillPosition) &&
                Objects.equals(livePosition, that.livePosition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(backfillPosition, livePosition);
    }

    @Override
    public String toString() {
        return "Stitched{backfill:" + backfillPosition + ";live:" + livePosition + '}';
    }
}
