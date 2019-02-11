package com.timgroup.eventstore.filesystem;

import javax.annotation.Nonnull;
import java.io.IOException;

public final class WrappedIOException extends RuntimeException {
    @Nonnull
    private final IOException ioException;

    public WrappedIOException(@Nonnull IOException e) {
        super(e);
        this.ioException = e;
    }

    @Nonnull
    public IOException getIoException() {
        return ioException;
    }
}
