package com.timgroup.eventsubscription.healthcheck;

import com.timgroup.eventsubscription.lifecycleevents.CaughtUp;
import com.timgroup.eventsubscription.lifecycleevents.InitialCatchupCompleted;
import com.timgroup.eventsubscription.lifecycleevents.SubscriptionTerminated;
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

    private final CountDownLatch latch = new CountDownLatch(1);
    private final ExecutorService executorService = Executors.newFixedThreadPool(1);

    @After public void
    cleanup() {
        executorService.shutdown();
    }

    @Test public void
    awaits_for_caught_up() throws InterruptedException {
        ServerSubscriptionListener listener = new ServerSubscriptionListener();
        listener.apply(new TestPosition(1), new InitialCatchupCompleted(new TestPosition(1), null));

        executorService.submit(() -> {
            listener.await(new TestPosition(2), TestPosition.CODEC);
            latch.countDown();
        });

        listener.apply(new TestPosition(2), new CaughtUp(new TestPosition(1), null));
        if (!latch.await(200, TimeUnit.MILLISECONDS))
            throw new AssertionError("listener await didn't complete");
    }

    @Test public void
    rethrows_termination_exception() {
        ServerSubscriptionListener listener = new ServerSubscriptionListener();
        Exception expected = new Exception("Termination exception");

        listener.apply(new TestPosition(1), new SubscriptionTerminated(new TestPosition(1), expected));

        try {
            listener.await(new TestPosition(Long.MAX_VALUE), TestPosition.CODEC);
        } catch (IllegalStateException e) {
            assertThat(e.getCause(), equalTo(expected));
            return;
        } catch (Exception e) {
            fail("Unexpected exception was thrown: " + e);
        }

        fail("Expecting termination exception to be rethrown but it was not");
    }
}