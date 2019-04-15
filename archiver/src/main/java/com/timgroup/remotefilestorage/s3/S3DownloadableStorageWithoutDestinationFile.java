package com.timgroup.remotefilestorage.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.timgroup.remotefilestorage.api.DownloadableStorage;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.function.Function;

public class S3DownloadableStorageWithoutDestinationFile implements DownloadableStorage {

    private final DownloadableStorage delegate;
    private final AmazonS3 amazonS3;
    private final String bucketName;

    public S3DownloadableStorageWithoutDestinationFile(DownloadableStorage delegate, AmazonS3 amazonS3, String bucketName) {
        this.delegate = delegate;
        this.amazonS3 = amazonS3;
        this.bucketName = bucketName;
    }

    @Override
    public void download(String key, Path destination) {
        delegate.download(key, destination);
    }

    @Override
    public void download(URI uri, Path destination) {
        delegate.download(uri, destination);
    }

    public <T> T download(String key, Function<InputStream, T> reader) {
        try (S3Object s3Object = amazonS3.getObject(new GetObjectRequest(bucketName, key));
             S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent()) {
            return reader.apply(s3ObjectInputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
