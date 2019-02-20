package com.timgroup.eventstore.filesystem.archivetool;

import com.codahale.metrics.MetricRegistry;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.filesystem.IncrementalEventArchiver;
import com.timgroup.eventstore.mysql.BasicMysqlEventSource;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public final class ArchiveTool {
    public static void main(String[] args) {
        Options options = parseOptions(args);

        EventSource eventSource;
        try {
            eventSource = options.getSource().buildEventSource();
        } catch (IOException e) {
            System.err.println("Failed to initialise event source");
            e.printStackTrace(System.err);
            System.exit(2);
            throw new IllegalStateException();
        }

        IncrementalEventArchiver eventArchiver = new IncrementalEventArchiver(eventSource, options.getArchiveDirectory(), options.isInitialiseNew());
        try {
            eventArchiver.archiveEvents();
        } catch (IOException e) {
            System.err.println("Failed to archive events");
            e.printStackTrace(System.err);
            System.exit(1);
            throw new IllegalStateException();
        }

        System.exit(0);
    }

    // archivetool [-d directory] [-init] jdbc-url jdbc-user jdbc-password table-name
    private static Options parseOptions(String[] args) {
        if (args.length < 3) {
            System.err.println("Syntax: archivetool [-d directory] [-init] config.properties db-tag table-name");
            System.exit(100);
        }

        List<String> argList = new ArrayList<String>(Arrays.asList(args));
        Path directory = Paths.get(".");
        boolean initialiseNew = false;

        while (!argList.isEmpty()) {
            String next = argList.get(0);
            if (next.equals("-d")) {
                argList.remove(0);
                directory = Paths.get(argList.remove(0));
            }
            else if (next.equals("-init")) {
                initialiseNew = true;
                argList.remove(0);
            }
            else {
                break;
            }
        }

        String propertiesFilename = argList.remove(0);
        String databaseTag = argList.remove(0);
        String tableName = argList.remove(0);

        return new Options(initialiseNew, new EventSourceConfig(Paths.get(propertiesFilename), String.format("db.%s.", databaseTag), databaseTag, tableName), directory);
    }

    public static final class Options {
        private final boolean initialiseNew;
        @Nonnull private final EventSourceConfig source;
        @Nonnull private final Path archiveDirectory;

        public Options(boolean initialiseNew, @Nonnull EventSourceConfig source, @Nonnull Path archiveDirectory) {
            this.initialiseNew = initialiseNew;
            this.source = source;
            this.archiveDirectory = archiveDirectory;
        }

        public boolean isInitialiseNew() {
            return initialiseNew;
        }

        @Nonnull
        public EventSourceConfig getSource() {
            return source;
        }

        @Nonnull
        public Path getArchiveDirectory() {
            return archiveDirectory;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Options options = (Options) o;
            return initialiseNew == options.initialiseNew &&
                    source.equals(options.source) &&
                    archiveDirectory.equals(options.archiveDirectory);
        }

        @Override
        public int hashCode() {
            return Objects.hash(initialiseNew, source, archiveDirectory);
        }

        @Override
        public String toString() {
            return "Options{" +
                    "initialiseNew=" + initialiseNew +
                    ", source=" + source +
                    ", archiveDirectory=" + archiveDirectory +
                    '}';
        }
    }

    public static final class EventSourceConfig {
        @Nonnull private final Path propertiesFile;
        @Nonnull private final String propertiesPrefix;
        @Nonnull private final String sourceName;
        @Nonnull private final String tableName;
        @Nonnull private final MetricRegistry metrics = new MetricRegistry();

        public EventSourceConfig(@Nonnull Path propertiesFile, @Nonnull String propertiesPrefix, @Nonnull String sourceName, @Nonnull String tableName) {
            this.propertiesFile = propertiesFile;
            this.propertiesPrefix = propertiesPrefix;
            this.sourceName = sourceName;
            this.tableName = tableName;
        }

        @Nonnull
        public EventSource buildEventSource() throws IOException {
            try (InputStream inputStream = Files.newInputStream(propertiesFile)) {
                Properties properties = new Properties();
                properties.load(inputStream);
                if (properties.getProperty(propertiesPrefix + "read_only_cluster") != null) {
                    return BasicMysqlEventSource.pooledReadOnlyDbEventSource(properties, propertiesPrefix, tableName, sourceName, metrics);
                }
                else {
                    return BasicMysqlEventSource.pooledMasterDbEventSource(properties, propertiesPrefix, tableName, sourceName, metrics);
                }
            }
        }

    }

    private ArchiveTool() { }
}
