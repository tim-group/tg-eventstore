package com.timgroup.eventstore.filesystem;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class AbstractIterator<T> implements Iterator<T> {
    private enum State { EMPTY, FULL, FINISHED }

    private State state = State.EMPTY;
    private T nextValue;

    public abstract void computeNext();

    protected final void setNext(T value) {
        nextValue = value;
        state = State.FULL;
    }

    protected final void done() {
        state = State.FINISHED;
    }

    @Override
    public final boolean hasNext() {
        if (state == State.EMPTY) {
            tryToComputeNext();
        }
        return state == State.FULL;
    }

    @Override
    public final T next() {
        if (state == State.EMPTY)
            tryToComputeNext();
        if (state == State.FINISHED)
            throw new NoSuchElementException();
        T valueToReturn = nextValue;
        nextValue = null;
        state = State.EMPTY;
        return valueToReturn;
    }

    private void tryToComputeNext() {
        computeNext();
        if (state == State.EMPTY) {
            throw new IllegalStateException("computeNext() should have called setNext(T) or done()");
        }
    }
}
