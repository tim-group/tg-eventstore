package com.timgroup.eventstore.archiver;

import net.ttsui.junit.rules.pending.PendingRule;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assume.assumeTrue;

public class S3IntegrationTest {
    public static final String S3_PROPERTIES_FILE = "s3_do_not_check_in.properties";

    @Rule
    public PendingRule pendingRule = new PendingRule();

    @Rule public TestName testNameRule = new TestName();

    @BeforeClass
    public static void verifyS3CredentialsSupplied() {
        assumeTrue("S3 credentials must be supplied via properties file", Files.exists(Paths.get(S3_PROPERTIES_FILE)));
    }

    private final Long descendingCounter = Long.MAX_VALUE - System.currentTimeMillis();

    protected static String randomCategory() {
        return "stream_" + UUID.randomUUID().toString().replace("-", "");
    }

    protected static byte[] randomData() {
        return ("{\n  \"value\": \"" + UUID.randomUUID() + "\"\n}").getBytes(UTF_8);
    }

    protected static void completeOrFailAfter(CompletableFuture<?> toComplete, Duration timeout) {
        CompletableFuture<Void> completion = new CompletableFuture<>();
        Timer timer = new Timer(true);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                completion.completeExceptionally(new TimeoutException());
            }
        }, timeout.toMillis());

        CompletableFuture.anyOf(toComplete, completion).join();
    }


    protected String uniqueEventStoreId(String testClassName) {
        return "test-eventstore-" + descendingCounter + "-" + testClassName + "_" + testNameRule.getMethodName() + "-" + RandomStringUtils.randomAlphabetic(10);
    }
}
