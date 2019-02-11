package com.timgroup.eventstore.filesystem;

import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.memory.InMemoryEventSource;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import org.apache.commons.compress.archivers.cpio.CpioArchiveEntry;
import org.apache.commons.compress.archivers.cpio.CpioArchiveInputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

public class EventArchiverTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final Clock clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);
    private final EventSource eventSource = new InMemoryEventSource(new JavaInMemoryEventStore(clock));

    {
        eventSource.writeStream().write(streamId("testCategory", "testId"), Arrays.asList(
                newEvent("EventType", "data".getBytes()),
                newEvent("EventType", "data".getBytes(), "metadata".getBytes())
        ));
        eventSource.writeStream().write(streamId("testCategory", "otherTestId"), singletonList(
                newEvent("EventType", "data".getBytes())
        ));
        eventSource.writeStream().write(streamId("otherTestCategory", "testId"), singletonList(
                newEvent("EventType", "data".getBytes())
        ));
    }

    @Test
    public void archives_single_stream() throws IOException {
        Path tempFile = temporaryFolder.newFile("events.cpio").toPath();
        new EventArchiver(eventSource).archiveStream(tempFile, streamId("testCategory", "testId"));

        assertThat(cpioMembersOf(tempFile), contains(
            new CpioMember("00000000.testCategory.testId.0.EventType.data", Instant.EPOCH),
            new CpioMember("00000001.testCategory.testId.1.EventType.data", Instant.EPOCH),
            new CpioMember("00000001.testCategory.testId.1.EventType.metadata", Instant.EPOCH)
        ));
    }

    @Test
    public void archives_single_category() throws IOException {
        Path tempFile = temporaryFolder.newFile("events.cpio").toPath();
        new EventArchiver(eventSource).archiveCategory(tempFile, "testCategory");

        assertThat(cpioMembersOf(tempFile), contains(
            new CpioMember("00000000.testCategory.testId.0.EventType.data", Instant.EPOCH),
            new CpioMember("00000001.testCategory.testId.1.EventType.data", Instant.EPOCH),
            new CpioMember("00000001.testCategory.testId.1.EventType.metadata", Instant.EPOCH),
            new CpioMember("00000002.testCategory.otherTestId.0.EventType.data", Instant.EPOCH)
        ));
    }

    @Test
    public void archives_entire_store() throws IOException {
        Path tempFile = temporaryFolder.newFile("events.cpio").toPath();
        new EventArchiver(eventSource).archiveStore(tempFile);

        assertThat(cpioMembersOf(tempFile), contains(
            new CpioMember("00000000.testCategory.testId.0.EventType.data", Instant.EPOCH),
            new CpioMember("00000001.testCategory.testId.1.EventType.data", Instant.EPOCH),
            new CpioMember("00000001.testCategory.testId.1.EventType.metadata", Instant.EPOCH),
            new CpioMember("00000002.testCategory.otherTestId.0.EventType.data", Instant.EPOCH),
            new CpioMember("00000003.otherTestCategory.testId.0.EventType.data", Instant.EPOCH)
        ));
    }

    private static final class CpioMember {
        private final String filename;
        private final Instant timestamp;

        public CpioMember(String filename, Instant timestamp) {
            this.filename = filename;
            this.timestamp = timestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CpioMember that = (CpioMember) o;
            return Objects.equals(filename, that.filename) &&
                    Objects.equals(timestamp, that.timestamp);
        }

        @Override
        public int hashCode() {
            return Objects.hash(filename, timestamp);
        }

        @Override
        public String toString() {
            return "CpioMember{" +
                    "filename='" + filename + '\'' +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }

    private static List<CpioMember> cpioMembersOf(Path path) throws IOException {
        try (CpioArchiveInputStream cpioInput = new CpioArchiveInputStream(Files.newInputStream(path))) {
            CpioArchiveEntry entry;
            List<CpioMember> output = new ArrayList<>();
            while ((entry = cpioInput.getNextCPIOEntry()) != null) {
                output.add(new CpioMember(entry.getName(), Instant.ofEpochMilli(entry.getTime())));
            }
            return output;
        }
    }
}
