package com.timgroup.eventstore.archiver.monitoring;

import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.Report;
import com.timgroup.tucker.info.Status;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetBucketEncryptionRequest;
import software.amazon.awssdk.services.s3.model.GetBucketEncryptionResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class S3ArchiveBucketConfigurationComponentTest {

    @Test
    public void
    is_ok_when_bucket_is_encrypted() {
        S3Client amazonS3 = mock(S3Client.class);
        when(amazonS3.getBucketEncryption(ArgumentMatchers.any(Consumer.class))).thenReturn(GetBucketEncryptionResponse.builder()
                .serverSideEncryptionConfiguration(
                        c -> c.rules(
                                r -> r.applyServerSideEncryptionByDefault(d -> d.sseAlgorithm(ServerSideEncryption.AES256))
                        )
                )
                .build());

        Component encryptedBucketComponent = new S3ArchiveBucketConfigurationComponent(amazonS3, "encrypted");

        Report report = encryptedBucketComponent.getReport();
        assertThat(report.getStatus(), equalTo(Status.OK));
        assertThat(report.getValue().toString(), allOf(containsString("is encrypted"), containsString("AES256")));
    }

    @Test
    public void
    warns_when_bucket_is_not_encrypted() {
        S3Client amazonS3 = mock(S3Client.class);
        when(amazonS3.getBucketEncryption(ArgumentMatchers.any(Consumer.class))).thenThrow(S3Exception.builder().message("No encryption").build());

        Component unencryptedBucketComponent = new S3ArchiveBucketConfigurationComponent(amazonS3, "unencrypted");

        Report unencryptedBucketReport = unencryptedBucketComponent.getReport();
        assertThat(unencryptedBucketReport.getStatus(), equalTo(Status.WARNING));
        assertThat(unencryptedBucketReport.getValue().toString(), containsString("Could not verify default server-side encryption algorithm AES256"));
    }

    @Test
    public void
    includes_bucket_name_in_output() {
        S3Client s3client = mock(S3Client.class);
        when(s3client.getBucketEncryption(ArgumentMatchers.any(Consumer.class))).thenThrow(S3Exception.builder().message("No encryption").build());

        Component component = new S3ArchiveBucketConfigurationComponent(s3client, "My Lovely Bucket");

        assertThat(component.getLabel(), containsString("My Lovely Bucket"));
    }
}