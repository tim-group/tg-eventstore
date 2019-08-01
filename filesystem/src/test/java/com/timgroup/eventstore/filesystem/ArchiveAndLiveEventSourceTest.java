package com.timgroup.eventstore.filesystem;

import com.timgroup.eventstore.api.NewEvent;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.memory.InMemoryEventSource;
import com.youdevise.testutils.matchers.Contains;
import org.apache.commons.compress.archivers.cpio.CpioArchiveEntry;
import org.apache.commons.compress.archivers.cpio.CpioArchiveOutputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static com.timgroup.eventstore.api.EventRecord.eventRecord;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;

public class ArchiveAndLiveEventSourceTest {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void reads_store_from_archive_sequence_followed_by_live_events() throws Exception {
        Instant liveEventTime = Instant.parse("2019-02-20T13:00:00Z");
        try (CpioArchiveOutputStream cpioOutput = new CpioArchiveOutputStream(Files.newOutputStream(
                temporaryFolder.newFile("00000001.testCategory.testId.1.EventType.cpio").toPath()))) {
            writeEntry(cpioOutput, "00000000.testCategory.testId.0.EventType.data", "xyzzy");
            writeEntry(cpioOutput, "00000001.testCategory.testId.1.EventType.data", "abcde");
            writeEntry(cpioOutput, "00000001.testCategory.testId.1.EventType.metadata", "12345");
        }
        Files.write(temporaryFolder.newFile("00000001.testCategory.testId.1.EventType.position.txt").toPath(), "2".getBytes(UTF_8));

        try (CpioArchiveOutputStream cpioOutput = new CpioArchiveOutputStream(Files.newOutputStream(
                temporaryFolder.newFile("00000002.testCategory.testId.2.EventType.cpio").toPath()))) {
            writeEntry(cpioOutput, "00000002.testCategory.testId.2.EventType.data", "nnnnn");
        }
        Files.write(temporaryFolder.newFile("00000002.testCategory.testId.2.EventType.position.txt").toPath(), "3".getBytes(UTF_8));

        InMemoryEventSource memoryEventSource = new InMemoryEventSource(Clock.fixed(liveEventTime, UTC));
        memoryEventSource.writeStream().write(streamId("testCategory", "testId"), Arrays.asList(
                NewEvent.newEvent("EventType", "xyzzy".getBytes(), "".getBytes()),
                NewEvent.newEvent("EventType", "abcde".getBytes(), "12345".getBytes()),
                NewEvent.newEvent("EventType", "nnnnn".getBytes(), "".getBytes()),
                NewEvent.newEvent("EventType", "zzzzz".getBytes(), "aaaaa".getBytes())
        ));

        ArchiveAndLiveEventSource eventSource = new ArchiveAndLiveEventSource(temporaryFolder.getRoot().toPath(), memoryEventSource);

        List<ResolvedEvent> events = eventSource.readAll().readAllForwards().collect(toList());

        assertThat(events, Contains.inOrder(
                eventRecord(Instant.EPOCH, streamId("testCategory", "testId"), 0, "EventType", "xyzzy".getBytes(), "".getBytes())
                        .toResolvedEvent(eventSource.readAll().storePositionCodec().deserializePosition("00000001.testCategory.testId.1.EventType.cpio:00000000.testCategory.testId.0.EventType")),
                eventRecord(Instant.EPOCH, streamId("testCategory", "testId"), 1, "EventType", "abcde".getBytes(), "12345".getBytes())
                        .toResolvedEvent(eventSource.readAll().storePositionCodec().deserializePosition("00000001.testCategory.testId.1.EventType.cpio:00000001.testCategory.testId.1.EventType")),
                eventRecord(Instant.EPOCH, streamId("testCategory", "testId"), 2, "EventType", "nnnnn".getBytes(), "".getBytes())
                        .toResolvedEvent(eventSource.readAll().storePositionCodec().deserializePosition("00000002.testCategory.testId.2.EventType.cpio:00000002.testCategory.testId.2.EventType")),
                eventRecord(liveEventTime, streamId("testCategory", "testId"), 3, "EventType", "zzzzz".getBytes(), "aaaaa".getBytes())
                        .toResolvedEvent(eventSource.readAll().storePositionCodec().deserializePosition("00000002.testCategory.testId.2.EventType.cpio:00000002.testCategory.testId.2.EventType:4"))
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
