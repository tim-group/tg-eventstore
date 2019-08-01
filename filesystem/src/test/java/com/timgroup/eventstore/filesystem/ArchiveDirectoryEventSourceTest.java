package com.timgroup.eventstore.filesystem;

import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.youdevise.testutils.matchers.Contains;
import org.apache.commons.compress.archivers.cpio.CpioArchiveEntry;
import org.apache.commons.compress.archivers.cpio.CpioArchiveOutputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;

public class ArchiveDirectoryEventSourceTest {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void reads_store_from_archive_sequence() throws Exception {
        try (CpioArchiveOutputStream cpioOutput = new CpioArchiveOutputStream(Files.newOutputStream(
                temporaryFolder.newFile("00000001.testCategory.testId.1.EventType.cpio").toPath()))) {
            writeEntry(cpioOutput, "00000000.testCategory.testId.0.EventType.data", "xyzzy");
            writeEntry(cpioOutput, "00000001.testCategory.testId.1.EventType.data", "abcde");
            writeEntry(cpioOutput, "00000001.testCategory.testId.1.EventType.metadata", "12345");
        }
        Files.write(temporaryFolder.newFile("00000001.testCategory.testId.1.EventType.position.txt").toPath(), "1".getBytes(UTF_8));

        try (CpioArchiveOutputStream cpioOutput = new CpioArchiveOutputStream(Files.newOutputStream(
                temporaryFolder.newFile("00000002.testCategory.testId.2.EventType.cpio").toPath()))) {
            writeEntry(cpioOutput, "00000002.testCategory.testId.2.EventType.data", "nnnnn");
        }
        Files.write(temporaryFolder.newFile("00000002.testCategory.testId.2.EventType.position.txt").toPath(), "2".getBytes(UTF_8));

        ArchiveDirectoryEventSource eventSource = new ArchiveDirectoryEventSource(temporaryFolder.getRoot().toPath());
        EventReader archiveReader = eventSource.readAll();

        List<ResolvedEvent> events = archiveReader.readAllForwards().collect(toList());

        assertThat(events, Contains.inOrder(
                eventRecord(Instant.EPOCH, streamId("testCategory", "testId"), 0, "EventType", "xyzzy".getBytes(), "".getBytes())
                        .toResolvedEvent(archiveReader.storePositionCodec().deserializePosition("00000001.testCategory.testId.1.EventType.cpio:00000000.testCategory.testId.0.EventType")),
                eventRecord(Instant.EPOCH, streamId("testCategory", "testId"), 1, "EventType", "abcde".getBytes(), "12345".getBytes())
                        .toResolvedEvent(archiveReader.storePositionCodec().deserializePosition("00000001.testCategory.testId.1.EventType.cpio:00000001.testCategory.testId.1.EventType")),
                eventRecord(Instant.EPOCH, streamId("testCategory", "testId"), 2, "EventType", "nnnnn".getBytes(), "".getBytes())
                        .toResolvedEvent(archiveReader.storePositionCodec().deserializePosition("00000002.testCategory.testId.2.EventType.cpio:00000002.testCategory.testId.2.EventType"))
        ));
    }

    @Test
    public void reads_store_from_specified_start_point() throws Exception {
        try (CpioArchiveOutputStream cpioOutput = new CpioArchiveOutputStream(Files.newOutputStream(
                temporaryFolder.newFile("00000001.testCategory.testId.1.EventType.cpio").toPath()))) {
            writeEntry(cpioOutput, "00000000.testCategory.testId.0.EventType.data", "xyzzy");
            writeEntry(cpioOutput, "00000001.testCategory.testId.1.EventType.data", "abcde");
            writeEntry(cpioOutput, "00000001.testCategory.testId.1.EventType.metadata", "12345");
        }
        Files.write(temporaryFolder.newFile("00000001.testCategory.testId.1.EventType.position.txt").toPath(), "1".getBytes(UTF_8));

        ArchiveDirectoryEventSource eventSource = new ArchiveDirectoryEventSource(temporaryFolder.getRoot().toPath());
        EventReader archiveReader = eventSource.readAll();

        List<ResolvedEvent> initialEvents = archiveReader.readAllForwards().collect(toList());

        Position startExclusive = initialEvents.get(initialEvents.size() - 1).position();

        try (CpioArchiveOutputStream cpioOutput = new CpioArchiveOutputStream(Files.newOutputStream(
                temporaryFolder.newFile("00000002.testCategory.testId.2.EventType.cpio").toPath()))) {
            writeEntry(cpioOutput, "00000002.testCategory.testId.2.EventType.data", "nnnnn");
        }
        Files.write(temporaryFolder.newFile("00000002.testCategory.testId.2.EventType.position.txt").toPath(), "2".getBytes(UTF_8));

        List<ResolvedEvent> events = archiveReader.readAllForwards(startExclusive).collect(toList());

        assertThat(events, Contains.inOrder(
                eventRecord(Instant.EPOCH, streamId("testCategory", "testId"), 2, "EventType", "nnnnn".getBytes(), "".getBytes())
                        .toResolvedEvent(archiveReader.storePositionCodec().deserializePosition("00000002.testCategory.testId.2.EventType.cpio:00000002.testCategory.testId.2.EventType"))
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
