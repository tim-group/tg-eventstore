package com.timgroup.eventstore.archiver;

import net.ttsui.junit.rules.pending.PendingRule;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.nio.file.Files;
import java.nio.file.Paths;

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


    protected String uniqueEventStoreId(String testClassName) {
        return "test-eventstore-" + descendingCounter + "-" + testClassName + "." + testNameRule.getMethodName() + "-" + RandomStringUtils.randomAlphabetic(10);
    }
}
