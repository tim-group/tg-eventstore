package com.timgroup.filesystem.filefeedcache;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.archiver.S3ArchivePosition;
import org.junit.Test;

import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class ArchiveToLivePositionTest {

    @Test public void can_compare_positions_in_the_archive_and_live_event_readers() {
        ArchiveToLivePosition archive1 = new ArchiveToLivePosition(new S3ArchivePosition(1));
        ArchiveToLivePosition archive2 = new ArchiveToLivePosition(new S3ArchivePosition(2));

        ArchiveToLivePosition live1 = new ArchiveToLivePosition(new LongPosition(1));
        ArchiveToLivePosition live2 = new ArchiveToLivePosition(new LongPosition(2));
        ArchiveToLivePosition live3 = new ArchiveToLivePosition(new LongPosition(3));

        PositionCodec codec = ArchiveToLivePosition.codec(S3ArchivePosition.CODEC, LongPosition.CODEC, archive2.underlying);

        assertThat(codec.comparePositions(archive1, archive1), equalTo(0));
        assertThat(codec.comparePositions(archive1, archive2), equalTo(-1));
        assertThat(codec.comparePositions(archive2, archive1), equalTo(1));

        assertThat(codec.comparePositions(archive2, live3), equalTo(-1));
        assertThat(codec.comparePositions(archive2, live2), equalTo(0));
        assertThat(codec.comparePositions(archive2, live1), equalTo(1));
    }

    @Test public void serialises_and_deserialises_positions() {
        ArchiveToLivePosition archive1 = new ArchiveToLivePosition(new S3ArchivePosition(1));
        ArchiveToLivePosition live2 = new ArchiveToLivePosition(new LongPosition(2));

        PositionCodec codec = ArchiveToLivePosition.codec(S3ArchivePosition.CODEC, LongPosition.CODEC, archive1.underlying);

        assertThat(codec.serializePosition(archive1), equalTo("1"));
        assertThat(codec.serializePosition(live2), equalTo("2"));

        assertThat(codec.deserializePosition("1"), equalTo(archive1));
        assertThat(codec.deserializePosition("2"), equalTo(live2));
    }

    private static final class LongPosition implements Position, Comparable<LongPosition> {
        private static final PositionCodec CODEC =
                PositionCodec.ofComparable(LongPosition.class, s -> new LongPosition(Long.parseLong(s)), Object::toString);

        private long value;

        LongPosition(long value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LongPosition that = (LongPosition) o;
            return value == that.value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }

        @Override
        public int compareTo(LongPosition other) {
            return Long.compare(this.value, other.value);
        }

        @Override
        public String toString() {
            return Long.toString(value);
        }
    }

}