package com.timgroup.eventstore.archiver.monitoring;

import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.Report;
import com.timgroup.tucker.info.Status;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetBucketEncryptionResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.ServerSideEncryptionRule;

import java.util.List;

import static java.util.stream.Collectors.joining;

public class S3ArchiveBucketConfigurationComponent extends Component {

    private final String bucketName;
    private final S3Client s3Client;

    public S3ArchiveBucketConfigurationComponent(S3Client s3Client, String bucketName) {
        super("tg-eventstore-s3-archiver-bucket-configuration", "tg-eventstore-s3-archive Bucket Configuration (bucket=[" + bucketName + "])");
        this.bucketName = bucketName;
        this.s3Client = s3Client;
    }

    @Override
    public Report getReport() {
        try {
            GetBucketEncryptionResponse bucketEncryption = s3Client.getBucketEncryption(r -> r.bucket(bucketName));
            List<ServerSideEncryptionRule> rules = bucketEncryption.serverSideEncryptionConfiguration().rules();

            if (rules.size() != 1) {
                return new Report(Status.WARNING, "Unexpected server side encryption rules found: " + rules.stream().map(Object::toString).collect(joining("\n")));
            } else {
                ServerSideEncryptionRule serverSideEncryptionRule = rules.get(0);
                ServerSideEncryption sseAlgorithm = serverSideEncryptionRule.applyServerSideEncryptionByDefault().sseAlgorithm();
                return sseAlgorithm == ServerSideEncryption.AES256
                        ? new Report(Status.OK, "Bucket is encrypted with expected server-side-algorithm: " + sseAlgorithm)
                        : new Report(Status.WARNING, "Bucket is not encrypted with AES256 as expected. Server-side-algorithm is: " + sseAlgorithm);
            }
        } catch (S3Exception e) {
            return new Report(Status.WARNING, "Could not verify default server-side encryption algorithm AES256 due to exception:\n"
                    + ComponentUtils.getStackTraceAsString(e));
        }

    }
}
