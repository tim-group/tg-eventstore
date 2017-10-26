package com.timgroup.eventstore.diffing.utils;

import java.io.*;
import java.util.Arrays;
import java.util.List;

public final class PrintWriters {

    public static PrintWriter newWriter(String fileName) throws IOException {
        return new PrintWriter(new BufferedWriter(new FileWriter(fileName)));
    }

    public static PrintWriter newTeeWriter(String fileName) throws IOException {
        return new PrintWriter(new MultiWriter(
                new FileWriter(fileName),
                new OutputStreamWriter(System.out)
        ));
    }

    private static final class MultiWriter extends Writer {
        private List<Writer> delegates;

        private MultiWriter(Writer... delegates){
            this.delegates = Arrays.asList(delegates);
        }

        @Override public void write(char cbuf[], int off, int len) throws IOException {
            for (Writer w: delegates) { w.write(cbuf, off, len); }
        }

        @Override public void flush() throws IOException {
            for (Writer w: delegates) { w.flush(); }
        }

        @Override public void close() throws IOException {
            for (Writer w: delegates) { w.close(); }
        }
    }
}
