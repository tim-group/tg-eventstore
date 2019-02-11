package com.timgroup.eventstore.filesystem;

import com.timgroup.clocks.testing.ManualClock;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.memory.InMemoryEventSource;
import com.timgroup.eventstore.memory.JavaInMemoryEventStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Stream;

import static com.timgroup.eventstore.api.NewEvent.newEvent;
import static com.timgroup.eventstore.api.StreamId.streamId;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class IncrementalEventArchiverTest {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void archives_entire_event_store_and_writes_marker() throws Exception {
        ManualClock clock = new ManualClock(Instant.EPOCH, ZoneOffset.UTC);
        EventSource eventSource = new InMemoryEventSource(new JavaInMemoryEventStore(clock));
        eventSource.writeStream().write(streamId("testCategory", "testId"), Arrays.asList(
                newEvent("EventType", "data".getBytes()),
                newEvent("EventType", "data".getBytes(), "metadata".getBytes())
        ));

        new IncrementalEventArchiver(eventSource, temporaryFolder.getRoot().toPath(), true).archiveEvents();

        assertThat(filenames(), containsInAnyOrder("00000001.testCategory.testId.1.EventType.position.txt", "00000001.testCategory.testId.1.EventType.cpio"));
        assertThat(Files.readAllLines(path("00000001.testCategory.testId.1.EventType.position.txt")), contains("2"));
    }

    @Test
    public void refuses_to_create_initial_archive_when_not_enabled() throws Exception {
        ManualClock clock = new ManualClock(Instant.EPOCH, ZoneOffset.UTC);
        EventSource eventSource = new InMemoryEventSource(new JavaInMemoryEventStore(clock));
        eventSource.writeStream().write(streamId("testCategory", "testId"), Arrays.asList(
                newEvent("EventType", "data".getBytes()),
                newEvent("EventType", "data".getBytes(), "metadata".getBytes())
        ));

        expectedException.expect(IllegalStateException.class);
        new IncrementalEventArchiver(eventSource, temporaryFolder.getRoot().toPath(), false).archiveEvents();
    }

    private Path path() {
        return temporaryFolder.getRoot().toPath();
    }

    private Path path(String filename) {
        return path().resolve(filename);
    }

    private Set<String> filenames() throws IOException {
        try (Stream<Path> paths = Files.list(path())) {
            return paths.map(p -> p.getFileName().toString()).collect(toSet());
        }
    }
}
