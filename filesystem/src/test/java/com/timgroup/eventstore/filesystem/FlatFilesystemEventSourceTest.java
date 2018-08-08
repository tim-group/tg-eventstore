package com.timgroup.eventstore.filesystem;

import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.JavaEventStoreTest;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.time.Clock;

public class FlatFilesystemEventSourceTest extends JavaEventStoreTest {
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    @Override
    public EventSource eventSource() {
        return new FlatFilesystemEventSource(folder.getRoot().toPath(), Clock.systemDefaultZone(), ".json");
    }

    @Test
    @Ignore
    @Override
    public void can_read_multiple_categories_in_one_request() {
        super.can_read_multiple_categories_in_one_request();
    }
}
