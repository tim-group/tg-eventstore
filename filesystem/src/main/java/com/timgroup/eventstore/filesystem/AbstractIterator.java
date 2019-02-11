package com.timgroup.eventstore.filesystem;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

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

    @Override
    public final void forEachRemaining(Consumer<? super T> action) {
        requireNonNull(action);
        if (state == State.EMPTY)
            tryToComputeNext();
        while (state != State.FINISHED) {
            action.accept(nextValue);
            state = State.EMPTY;
            tryToComputeNext();
        }
    }

    @Override
    public final void remove() {
        throw new UnsupportedOperationException();
    }

    private void tryToComputeNext() {
        computeNext();
        if (state == State.EMPTY) {
            throw new IllegalStateException("computeNext() should have called setNext(T) or done()");
        }
    }
}
