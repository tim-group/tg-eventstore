package com.timgroup.eventstore.diffing;

import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.diffing.utils.ForkedStreamsLocator;
import com.timgroup.eventstore.diffing.utils.ForkedStreamsLocator.StreamPair;
import com.timgroup.eventstore.diffing.listeners.BroadcastingDiffListener;
import com.timgroup.eventstore.diffing.listeners.SamplingDiffListener;
import com.timgroup.eventstore.diffing.listeners.SummarisingDiffListener;
import com.timgroup.eventstore.diffing.utils.PrintWriters;
import com.timgroup.eventstore.mysql.BasicMysqlEventSource;

import java.io.*;
import java.time.Instant;
import java.util.Properties;

public final class DiffForkApp {

    private static void printUsage() {
        System.err.println("Diffs a forked event stream with its main line.");
        System.err.println("Usage: " + DiffForkApp.class.getName() + " <config-file> <db-config-key> <db-table> [<fork-stream-category>] [<fork-stream-id>]");
        System.err.println("  <config-file>: path to config file, e.g. \"/opt/apps/config.properties\" (required)");
        System.err.println("  <db-config-key>: config key of DB that contains fork, e.g. \"indicatoreventdb\" (required)");
        System.err.println("  <db-table>: table name that contains fork, e.g. \"Event_dayZeroRr27_version1point7\" (required)");
        System.err.println("  <fork-stream-category>: stream category of fork (optional, default \"all\")");
        System.err.println("  <fork-stream-id>: stream id of fork (optional, default \"all_1\")");
    }

    public static void main(String[] args) throws Throwable {
        if (args.length < 3) {
            printUsage();
            System.exit(1);
        }
        Properties properties = loadProps(args[0]);
        String dbKeyArg = args[1];
        String dbTableArg = args[2];
        String streamCategoryArg = args.length > 3 ? args[3] : "all";
        String streamIdArg = args.length > 4 ? args[4] : "all_1";

        System.out.println(Instant.now() + " locating start of fork...");
        EventSource eventSourceReadMany = BasicMysqlEventSource.pooledReadOnlyDbEventSource(properties, dbKeyArg, dbTableArg, dbKeyArg + "->" + dbTableArg, 50000);
        EventSource eventSourceReadOne = BasicMysqlEventSource.pooledReadOnlyDbEventSource(properties, dbKeyArg, dbTableArg, dbKeyArg + "->" + dbTableArg + "(batchSize 1)", 1);
        StreamId forkStreamId = StreamId.streamId(streamCategoryArg, streamIdArg);
        StreamPair<ResolvedEvent> streamPair = ForkedStreamsLocator.locateForkedStreams(eventSourceReadOne, eventSourceReadMany, forkStreamId);

        SummarisingDiffListener summarizingListener = new SummarisingDiffListener(PrintWriters.newTeeWriter("diffForkSummary.md"));
        EventStreamDiffer differ = new EventStreamDiffer(new BroadcastingDiffListener(
                summarizingListener,
                new SamplingDiffListener(
                        PrintWriters.newWriter("similarInOrig.tsv"),
                        PrintWriters.newWriter("similarInFork.tsv"),
                        PrintWriters.newWriter("unmatchedInOrig.tsv"),
                        PrintWriters.newWriter("unmatchedInFork.tsv")
                )
        ));

        differ.diff(streamPair.originalStream, streamPair.forkedStream);
        summarizingListener.printFinalReport();
    }

    static Properties loadProps(String configFilePath) {
        try (InputStream configProps = new FileInputStream(configFilePath)) {
            Properties properties = new Properties(System.getProperties());
            properties.load(configProps);
            return properties;
        } catch (IOException e) {
            throw new RuntimeException("error loading config file " + configFilePath, e);
        }
    }
}
