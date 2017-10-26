package com.timgroup.eventstore.diffing;

import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.diffing.listeners.BroadcastingDiffListener;
import com.timgroup.eventstore.diffing.listeners.SamplingDiffListener;
import com.timgroup.eventstore.diffing.listeners.SummarisingDiffListener;
import com.timgroup.eventstore.diffing.utils.PrintWriters;
import com.timgroup.eventstore.mysql.BasicMysqlEventSource;

import java.sql.DriverManager;
import java.util.Properties;

import static com.timgroup.eventstore.diffing.DiffForkApp.loadProps;

public final class DiffTablesApp {

    private static void printUsage() {
        System.err.println("Diffs the whole contents of two event store tables.");
        System.err.println("Usage: " + DiffTablesApp.class.getName() + " <config-file> <db-config-key-1> <db-table-1> <db-config-key-2> <db-table-2>");
        System.err.println("  <config-file>: path to config file, e.g. \"/opt/apps/config.properties\"");
        System.err.println("  <db-config-key-1>: config key of DB that contains first table to diff, e.g. \"indicatoreventdb\"");
        System.err.println("  <db-table-1>: name of first table to diff, e.g. \"Event_v1\"");
        System.err.println("  <db-config-key-2>: config key of DB that contains second table to diff, e.g. \"backtestdb\"");
        System.err.println("  <db-table-2>: name of second table to diff, e.g. \"Event_v2\"");
    }

    public static void main(String[] args) throws Throwable {
        if (args.length != 5) {
            printUsage();
            System.exit(1);
        }
        Properties properties = loadProps(args[0]);
        String dbKey1Arg = args[1];
        String dbTable1Arg = args[2];
        String dbKey2Arg = args[3];
        String dbTable2Arg = args[4];

        DriverManager.registerDriver(new com.mysql.jdbc.Driver());
        EventSource eventSource1 = BasicMysqlEventSource.pooledReadOnlyDbEventSource(properties, dbKey1Arg, dbTable1Arg, dbKey1Arg + "->" + dbTable1Arg);
        EventSource eventSource2 = BasicMysqlEventSource.pooledReadOnlyDbEventSource(properties, dbKey2Arg, dbTable2Arg, dbKey2Arg + "->" + dbTable2Arg);

        SummarisingDiffListener summarizingListener = new SummarisingDiffListener(PrintWriters.newTeeWriter("diffTablesSummary.md"));
        EventStreamDiffer differ = new EventStreamDiffer(new BroadcastingDiffListener(
                summarizingListener,
                new SamplingDiffListener(
                        PrintWriters.newWriter("similarIn1.tsv"),
                        PrintWriters.newWriter("similarIn2.tsv"),
                        PrintWriters.newWriter("unmatchedIn1.tsv"),
                        PrintWriters.newWriter("unmatchedIn2.tsv")
                )
        ));
        differ.diff(eventSource1.readAll().readAllForwards(), eventSource2.readAll().readAllForwards());
        summarizingListener.printFinalReport();
    }
}
