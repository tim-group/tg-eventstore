package com.timgroup.eventstore.archiver.monitoring;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetBucketEncryptionResult;
import com.amazonaws.services.s3.model.ServerSideEncryptionByDefault;
import com.amazonaws.services.s3.model.ServerSideEncryptionConfiguration;
import com.amazonaws.services.s3.model.ServerSideEncryptionRule;
import com.timgroup.config.ConfigLoader;
import com.timgroup.eventstore.archiver.S3ArchiveKeyFormat;
import com.timgroup.remotefilestorage.s3.S3ClientFactory;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.Report;
import com.timgroup.tucker.info.Status;
import org.junit.Test;

import java.util.Properties;
import java.util.stream.Stream;

import static org.mockito.Mockito.*;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class S3ArchiveBucketConfigurationComponentTest {

    @Test public void
    is_ok_when_bucket_is_encrypted() {
        AmazonS3 amazonS3 = mock(AmazonS3.class);
        when(amazonS3.getBucketEncryption("encrypted")).thenReturn(new GetBucketEncryptionResult().withServerSideEncryptionConfiguration(
                new ServerSideEncryptionConfiguration().withRules(new ServerSideEncryptionRule().withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault().withSSEAlgorithm("AES256")))));

        Component encryptedBucketComponent = new S3ArchiveBucketConfigurationComponent(amazonS3, "encrypted");

        Report report = encryptedBucketComponent.getReport();
        assertThat(report.getStatus(), equalTo(Status.OK));
        assertThat(report.getValue().toString(), allOf(containsString("is encrypted"), containsString("AES256")));
    }

    @Test public void
    warns_when_bucket_is_not_encrypted() {
        AmazonS3 amazonS3 = mock(AmazonS3.class);
        when(amazonS3.getBucketEncryption("unencrypted")).thenThrow(new AmazonS3Exception("No encryption"));

        Component unencryptedBucketComponent = new S3ArchiveBucketConfigurationComponent(amazonS3, "unencrypted");

        Report unencryptedBucketReport = unencryptedBucketComponent.getReport();
        assertThat(unencryptedBucketReport.getStatus(), equalTo(Status.WARNING));
        assertThat(unencryptedBucketReport.getValue().toString(), containsString("Could not verify default server-side encryption algorithm AES256"));
    }
}