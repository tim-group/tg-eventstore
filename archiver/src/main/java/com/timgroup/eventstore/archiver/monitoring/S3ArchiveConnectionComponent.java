package com.timgroup.eventstore.archiver.monitoring;

import com.timgroup.eventstore.archiver.S3ArchiveMaxPositionFetcher;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.Report;

import static com.timgroup.tucker.info.Status.CRITICAL;
import static com.timgroup.tucker.info.Status.OK;

public class S3ArchiveConnectionComponent extends Component {
    private final String eventStoreId;
    private final S3ArchiveMaxPositionFetcher maxPositionFetcher;

    public S3ArchiveConnectionComponent(String id, String label, String eventStoreId, S3ArchiveMaxPositionFetcher maxPositionFetcher) {
        super(id, label);
        this.eventStoreId = eventStoreId;
        this.maxPositionFetcher = maxPositionFetcher;
    }

    @Override
    public Report getReport() {
        try {
            return maxPositionFetcher.maxPosition()
                    .map(position -> new Report(OK, "Successfully connected to S3 EventStore, max position=" + position))
                    .orElseGet(() -> new Report(CRITICAL, "Successfully connected to S3 EventStore, but no EventStore with ID='" + eventStoreId + "' exists"));
        } catch (Exception e) {
            return new Report(CRITICAL, "Unable to connect to S3 Archive to retrieve max position\n"
                    + ComponentUtils.getStackTraceAsString(e));

        }
    }
}
