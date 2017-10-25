package com.timgroup.eventstore.filesystem;

import java.time.Clock;

import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.JavaEventStoreTest;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class FlatFilesystemEventSourceTest extends JavaEventStoreTest {
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    @Override
    public EventSource eventSource() {
        return new FlatFilesystemEventSource(folder.getRoot().toPath(), Clock.systemDefaultZone(), ".json");
    }
}
