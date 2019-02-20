package com.timgroup.eventstore.filesystem;

import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class ArchiveAndLivePosition implements Position {
    public static final ArchiveAndLivePosition EMPTY = new ArchiveAndLivePosition(ArchiveDirectoryPosition.EMPTY, null);

    static String serialise(ArchiveAndLivePosition position, PositionCodec livePositionCodec) {
        if (position.getLivePosition() == null) {
            return ArchiveDirectoryPosition.serialise(position.getArchiveDirectoryPosition());
        }
        else {
            return ArchiveDirectoryPosition.serialise(position.getArchiveDirectoryPosition()) + ":" + livePositionCodec.serializePosition(position.getLivePosition());
        }
    }

    static ArchiveAndLivePosition deserialise(String str, PositionCodec livePositionCodec) {
        int fileSeparator = str.indexOf(':');
        int memberSeparator = str.indexOf(':', fileSeparator + 1);
        if (memberSeparator < 0) {
            return new ArchiveAndLivePosition(ArchiveDirectoryPosition.deserialise(str), null);
        }
        else {
            ArchiveDirectoryPosition archiveDirectoryPosition = ArchiveDirectoryPosition.deserialise(str.substring(0, memberSeparator));
            Position livePosition = livePositionCodec.deserializePosition(str.substring(memberSeparator + 1));
            return new ArchiveAndLivePosition(archiveDirectoryPosition, livePosition);
        }
    }

    @Nonnull private final ArchiveDirectoryPosition archiveDirectoryPosition;
    @Nullable private final Position livePosition;

    public ArchiveAndLivePosition(@Nonnull ArchiveDirectoryPosition archiveDirectoryPosition, @Nullable Position livePosition) {
        this.archiveDirectoryPosition = archiveDirectoryPosition;
        this.livePosition = livePosition;
    }

    @Nonnull
    public ArchiveDirectoryPosition getArchiveDirectoryPosition() {
        return archiveDirectoryPosition;
    }

    @Nullable
    public Position getLivePosition() {
        return livePosition;
    }

    public ArchiveAndLivePosition withLivePosition(Position newLivePosition) {
        requireNonNull(newLivePosition, "newLivePosition");
        return new ArchiveAndLivePosition(archiveDirectoryPosition, newLivePosition);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ArchiveAndLivePosition that = (ArchiveAndLivePosition) o;
        return archiveDirectoryPosition.equals(that.archiveDirectoryPosition) &&
                Objects.equals(livePosition, that.livePosition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(archiveDirectoryPosition, livePosition);
    }

    @Override
    public String toString() {
        return "ArchiveAndLivePosition{" +
                "archiveDirectoryPosition=" + archiveDirectoryPosition +
                ", livePosition=" + livePosition +
                '}';
    }
}
