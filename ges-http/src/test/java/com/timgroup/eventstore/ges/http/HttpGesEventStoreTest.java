package com.timgroup.eventstore.ges.http;

import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.JavaEventStoreTest;
import org.junit.Ignore;

@Ignore
public class HttpGesEventStoreTest extends JavaEventStoreTest {
    @Override
    public EventSource eventSource() {
        return new HttpGesEventSource("http://localhost:2113");
    }
}
