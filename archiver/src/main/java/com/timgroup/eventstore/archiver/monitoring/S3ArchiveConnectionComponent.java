package com.timgroup.eventstore.archiver.monitoring;

import com.timgroup.eventstore.archiver.S3ArchiveMaxPositionFetcher;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.Report;
import com.timgroup.tucker.info.Status;

import java.util.Optional;

public class S3ArchiveConnectionComponent extends Component {
    private final S3ArchiveMaxPositionFetcher maxPositionFetcher;

    public S3ArchiveConnectionComponent(String id, String label, S3ArchiveMaxPositionFetcher maxPositionFetcher) {
        super(id, label);
        this.maxPositionFetcher = maxPositionFetcher;
    }

    @Override
    public Report getReport() {
        try {
            Optional<Long> maxPosition = maxPositionFetcher.maxPosition();
            return null;
        } catch (Exception e) {
            return new Report(Status.CRITICAL, "Unable to connect to S3 Archive to retrieve max position\n"
                    + ComponentUtils.getStackTraceAsString(e));

        }
    }
}
