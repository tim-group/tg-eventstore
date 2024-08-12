package com.timgroup.eventstore.archiver;

import java.io.InputStream;
import java.util.Map;

public final class S3BatchObject {
    public final byte[] content;
    public final int contentLength;
    public final Map<String, String> metadata;

    public S3BatchObject(byte[] content, int contentLength, Map<String, String> metadata) {
        this.content = content;
        this.contentLength = contentLength;
        this.metadata = metadata;
    }
}
