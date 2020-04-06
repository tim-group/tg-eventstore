package com.timgroup.filesystem.filefeedcache;

import com.timgroup.eventstore.archiver.monitoring.ComponentUtils;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.Report;

import java.time.Duration;

import static com.timgroup.tucker.info.Status.CRITICAL;
import static com.timgroup.tucker.info.Status.OK;
import static java.lang.String.format;

public class FileFeedCacheConnectionComponent extends Component {

    private final String eventStoreId;
    private final MaxPositionFetcher maxPositionFetcher;

    public FileFeedCacheConnectionComponent(String eventStoreId, MaxPositionFetcher maxPositionFetcher) {
        super("file-feed-cache-eventsource-archive-connection-" + eventStoreId, "File Feed Cache Event Store Archive eventstoreId=" + eventStoreId);
        this.eventStoreId = eventStoreId;
        this.maxPositionFetcher = maxPositionFetcher;
    }


    @Override
    public Report getReport() {
        long start = System.currentTimeMillis();
        try {
            return maxPositionFetcher.maxPosition()
                    .map(position -> new Report(OK, "Successfully connected to File Feed EventStore Archive, max position=" + position + ". " + timingInfo(start)))
                    .orElseGet(() -> new Report(CRITICAL, "Successfully connected to File Feed EventStore Archive, but no EventStore Archive with ID='" + eventStoreId + "' exists. " + timingInfo(start)));
        } catch (Exception e) {
            return new Report(CRITICAL, "Unable to connect to File Feed EventStore Archive to retrieve max position\n"
                    + ComponentUtils.getStackTraceAsString(e)
                    + "\n" + timingInfo(start));

        }
    }

    private String timingInfo(long start) {
        return format("Took %s", Duration.ofMillis(System.currentTimeMillis() - start));
    }
}
