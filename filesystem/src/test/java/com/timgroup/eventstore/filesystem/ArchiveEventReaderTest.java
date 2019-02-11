package com.timgroup.eventstore.filesystem;

import com.timgroup.eventstore.api.ResolvedEvent;
import org.apache.commons.compress.archivers.cpio.CpioArchiveEntry;
import org.apache.commons.compress.archivers.cpio.CpioArchiveOutputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

public class ArchiveEventReaderTest {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void reads_store_from_archive() throws Exception {
        Path tempFile = temporaryFolder.newFile("events.cpio").toPath();
        try (CpioArchiveOutputStream cpioOutput = new CpioArchiveOutputStream(Files.newOutputStream(tempFile))) {
            writeEntry(cpioOutput, "00000000.testCategory.testId.0.EventType.data", "xyzzy");
            writeEntry(cpioOutput, "00000001.testCategory.testId.1.EventType.data", "abcde");
            writeEntry(cpioOutput, "00000001.testCategory.testId.1.EventType.metadata", "12345");
            writeEntry(cpioOutput, "00000002.testCategory.testId.2.EventType.data", "nnnnn");
        }
        ArchiveEventReader eventReader = new ArchiveEventReader(tempFile);

        List<ResolvedEvent> events = eventReader.readAllForwards().collect(toList());

        assertThat(events, contains(
                eventRecord(Instant.EPOCH, streamId("testCategory", "testId"), 0, "EventType", "xyzzy".getBytes(), "".getBytes())
                        .toResolvedEvent(ArchivePosition.CODEC.deserializePosition("00000000.testCategory.testId.0.EventType")),
                eventRecord(Instant.EPOCH, streamId("testCategory", "testId"), 1, "EventType", "abcde".getBytes(), "12345".getBytes())
                        .toResolvedEvent(ArchivePosition.CODEC.deserializePosition("00000001.testCategory.testId.1.EventType")),
                eventRecord(Instant.EPOCH, streamId("testCategory", "testId"), 2, "EventType", "nnnnn".getBytes(), "".getBytes())
                        .toResolvedEvent(ArchivePosition.CODEC.deserializePosition("00000002.testCategory.testId.2.EventType"))
        ));
    }

    @Test
    public void reads_store_from_archive_starting_from_given_position() throws Exception {
        Path tempFile = temporaryFolder.newFile("events.cpio").toPath();
        try (CpioArchiveOutputStream cpioOutput = new CpioArchiveOutputStream(Files.newOutputStream(tempFile))) {
            writeEntry(cpioOutput, "00000000.testCategory.testId.0.EventType.data", "xyzzy");
            writeEntry(cpioOutput, "00000001.testCategory.testId.1.EventType.data", "abcde");
            writeEntry(cpioOutput, "00000001.testCategory.testId.1.EventType.metadata", "12345");
            writeEntry(cpioOutput, "00000002.testCategory.testId.2.EventType.data", "nnnnn");
        }
        ArchiveEventReader eventReader = new ArchiveEventReader(tempFile);

        List<ResolvedEvent> events = eventReader.readAllForwards(ArchivePosition.CODEC.deserializePosition("00000001.testCategory.testId.1.EventType")).collect(toList());

        assertThat(events, contains(
                eventRecord(Instant.EPOCH, streamId("testCategory", "testId"), 2, "EventType", "nnnnn".getBytes(), "".getBytes())
                        .toResolvedEvent(ArchivePosition.CODEC.deserializePosition("00000002.testCategory.testId.2.EventType"))
        ));
    }

    private static void writeEntry(CpioArchiveOutputStream cpioOutput, String filename, String content) throws IOException {
        byte[] contentBytes = content.getBytes();
        CpioArchiveEntry entry = new CpioArchiveEntry(filename);
        entry.setSize(contentBytes.length);
        cpioOutput.putArchiveEntry(entry);
        cpioOutput.write(contentBytes);
        cpioOutput.closeArchiveEntry();
    }
}
