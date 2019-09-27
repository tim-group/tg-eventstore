package com.timgroup.eventstore.archiver;

import java.util.Objects;
import java.util.Optional;

public final class ArchiverState {
    public final S3Archiver.RunState runState;
    public final Optional<Long> maxPositionInLive;
    public final Optional<Long> maxPositionInArchive;

    ArchiverState(S3Archiver.RunState runState, Optional<Long> maxPositionInLive, Optional<Long> maxPositionInArchive) {
        this.runState = runState;
        this.maxPositionInLive = maxPositionInLive;
        this.maxPositionInArchive = maxPositionInArchive;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ArchiverState that = (ArchiverState) o;
        return runState == that.runState &&
                maxPositionInLive.equals(that.maxPositionInLive) &&
                maxPositionInArchive.equals(that.maxPositionInArchive);
    }

    @Override
    public int hashCode() {
        return Objects.hash(runState, maxPositionInLive, maxPositionInArchive);
    }

    @Override
    public String toString() {
        return "ArchiverState{" +
                "runState=" + runState +
                ", maxPositionInLive=" + maxPositionInLive +
                ", maxPositionInArchive=" + maxPositionInArchive +
                '}';
    }
}
