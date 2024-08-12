package com.timgroup.remotefilestorage.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import org.slf4j.Logger;

import java.io.InputStream;
import java.net.URI;
import java.util.Map;

import static org.slf4j.LoggerFactory.getLogger;

public class S3UploadableStorageForInputStream implements UploadStorage {
    private static final Logger LOG = getLogger(S3UploadableStorage.class);

    private final AmazonS3 client;
    private final String bucketName;

    public S3UploadableStorageForInputStream(AmazonS3 client, String bucketName) {

        this.client = client;
        this.bucketName = bucketName;
    }

    @Override
    public URI upload(String name, InputStream content, long contentLength, Map<String, String> metaData) {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setUserMetadata(metaData);
        objectMetadata.setContentLength(contentLength);
        LOG.info("Creating upload request; bucket: [" + bucketName + "], inputStream: [" + content.toString() + "]");
        PutObjectRequest request = new PutObjectRequest(bucketName, name, content, objectMetadata);
        PutObjectResult response = client.putObject(request);
        URI uri = URI.create("s3://" + bucketName + "/" + name + (response.getVersionId() != null ? "?versionId=" + response.getVersionId() : ""));
        LOG.info("Response received, URI: " + uri);
        return uri;
    }
}
