package com.timgroup.eventstore.archiver.monitoring;


import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetBucketEncryptionResult;
import com.amazonaws.services.s3.model.ServerSideEncryptionRule;
import com.timgroup.eventstore.archiver.S3Archiver;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.Report;
import com.timgroup.tucker.info.Status;

import java.util.List;

import static java.util.stream.Collectors.joining;

public class S3ArchiveBucketConfigurationComponent extends Component {

    private final String bucketName;
    private final AmazonS3 s3Client;

    public S3ArchiveBucketConfigurationComponent(AmazonS3 s3Client, String bucketName) {
        super("tg-eventstore-s3-archiver-bucket-configuration", "tg-eventstore-s3-archive Bucket Configuration");
        this.bucketName = bucketName;
        this.s3Client = s3Client;
    }
    @Override
    public Report getReport() {
        try {
            GetBucketEncryptionResult bucketEncryption = s3Client.getBucketEncryption(bucketName);
            List<ServerSideEncryptionRule> rules = bucketEncryption.getServerSideEncryptionConfiguration().getRules();

            if (rules.size() != 1) {
                return new Report(Status.WARNING, "Unexpected server side encryption rules found: " + rules.stream().map(Object::toString).collect(joining("\n")));
            } else {
                ServerSideEncryptionRule serverSideEncryptionRule = rules.get(0);
                String sseAlgorithm = serverSideEncryptionRule.getApplyServerSideEncryptionByDefault().getSSEAlgorithm();
                return "AES256".equals(sseAlgorithm)
                        ? new Report(Status.OK, "Bucket is encrypted with expected server-side-algorithm: " + sseAlgorithm)
                        : new Report(Status.WARNING, "Bucket is not encrypted with AES256 as expected. Server-side-algorithm is: " + sseAlgorithm);
            }
        } catch (AmazonS3Exception e) {
            return new Report(Status.WARNING, "Could not verify default server-side encryption algorithm AES256 due to exception:\n"
                    + S3Archiver.getStackTraceAsString(e));
        }

    }
}
