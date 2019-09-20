package com.timgroup.eventstore.archiver.monitoring;

import java.io.PrintWriter;
import java.io.StringWriter;

public final class ComponentUtils {

    private ComponentUtils() {}

    /*
    Inline if/when Guava's Throwables.getStackTraceAsString is available.
     */
    public static String getStackTraceAsString(Throwable throwable) {
        StringWriter stringWriter = new StringWriter();
        throwable.printStackTrace(new PrintWriter(stringWriter));
        return stringWriter.toString();
    }
}
