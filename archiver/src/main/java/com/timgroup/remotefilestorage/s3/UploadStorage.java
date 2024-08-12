package com.timgroup.remotefilestorage.s3;

import java.io.InputStream;
import java.net.URI;
import java.util.Map;

public interface UploadStorage {
    URI upload(String name, InputStream content, long contentLength, Map<String, String> metaData);
}
