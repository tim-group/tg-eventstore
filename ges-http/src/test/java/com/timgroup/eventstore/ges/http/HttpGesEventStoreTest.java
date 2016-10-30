package com.timgroup.eventstore.ges.http;

import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.JavaEventStoreTest;

public class HttpGesEventStoreTest extends JavaEventStoreTest {
    @Override
    public EventSource eventSource() {
        return new HttpGesEventSource("http://192.168.99.100:32820");
    }
}
