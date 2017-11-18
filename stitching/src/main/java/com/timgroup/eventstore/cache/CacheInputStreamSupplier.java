package com.timgroup.eventstore.cache;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;
import java.util.zip.GZIPInputStream;

public class CacheInputStreamSupplier implements Supplier<DataInputStream> {

    private final File cacheFile;
    private final boolean compressed;
    private DataInputStream cacheInputStream = null;

    public CacheInputStreamSupplier(File cacheFile) {
        this(cacheFile, false);
    }

    public CacheInputStreamSupplier(File cacheFile, boolean compressed) {
        this.cacheFile = cacheFile;
        this.compressed = compressed;
    }

    @Override
    public DataInputStream get() {
        if (cacheInputStream == null) {
            cacheInputStream = loadCacheInputStream();
        }
        return cacheInputStream;
    }

    private DataInputStream loadCacheInputStream() {
        try {
            InputStream fileStream = new FileInputStream(cacheFile);
            if (compressed) {
                fileStream = new GZIPInputStream(fileStream);
            }
            return new DataInputStream(fileStream);
        } catch (IOException e) {
            throw new CacheNotFoundException("Unable to load cache: " + cacheFile, e);
        }
    }
}
