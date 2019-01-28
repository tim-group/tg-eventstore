package com.timgroup.eventsubscription.healthcheck;

import org.junit.After;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class ServerSubscriptionListenerTest {

    private final TestPositionCodec testPositionCodec = new TestPositionCodec();
    private final CountDownLatch latch = new CountDownLatch(1);
    private final ExecutorService executorService = Executors.newFixedThreadPool(1);

    @After public void
    cleanup() {
        executorService.shutdown();
    }

    @Test public void
    awaits_for_caught_up() throws InterruptedException {
        ServerSubscriptionListener listener = new ServerSubscriptionListener();
        listener.caughtUpAt(new TestPosition(1));

        executorService.submit(() -> {
            listener.await(new TestPosition(2), testPositionCodec);
            latch.countDown();
        });

        listener.caughtUpAt(new TestPosition(2));
        if (!latch.await(200, TimeUnit.MILLISECONDS))
            throw new AssertionError("listener await didn't complete");
    }

    @Test public void
    rethrows_termination_exception() {
        ServerSubscriptionListener listener = new ServerSubscriptionListener();
        Exception expected = new Exception("Termination exception");

        listener.terminated(new TestPosition(1), expected);

        try {
            listener.await(new TestPosition(Long.MAX_VALUE), testPositionCodec);
        } catch (IllegalStateException e) {
            assertThat(e.getCause(), equalTo(expected));
            return;
        } catch (Exception e) {
            fail("Unexpected exception was thrown: " + e);
        }

        fail("Expecting termination exception to be rethrown but it was not");
    }
}