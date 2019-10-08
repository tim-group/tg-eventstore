package com.timgroup.eventstore.archiver;

import com.codahale.metrics.Timer;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.archiver.monitoring.ComponentUtils;
import com.timgroup.eventsubscription.Event;
import com.timgroup.eventsubscription.EventHandler;
import com.timgroup.remotefilestorage.s3.S3UploadableStorageForInputStream;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.component.SimpleValueComponent;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.timgroup.tucker.info.Status.INFO;
import static java.lang.String.format;

final class BatchingUploadHandler implements EventHandler {
    private final S3UploadableStorageForInputStream output;
    private final Clock clock;
    private final CurrentBatchWriter currentBatchWriter;

    private final Map<String, String> appMetadata;
    private final Timer s3UploadTimer;
    private final SimpleValueComponent eventsAwaitingUploadComponent;
    private final SimpleValueComponent lastUploadState;

    BatchingUploadHandler(
            S3UploadableStorageForInputStream uploadableStorage,
            CurrentBatchWriter currentBatchWriter,
            Clock clock,
            Map<String, String> appMetadata,
            String monitoringPrefix,
            Timer s3UploadTimer)
    {
        this.output = uploadableStorage;
        this.clock = clock;
        this.currentBatchWriter = currentBatchWriter;

        this.appMetadata = appMetadata;
        this.s3UploadTimer = s3UploadTimer;
        this.eventsAwaitingUploadComponent = new SimpleValueComponent(monitoringPrefix + "-events-awaiting-upload", "Number of events awaiting upload to archive");
        this.eventsAwaitingUploadComponent.updateValue(INFO, 0);
        this.lastUploadState = new SimpleValueComponent(monitoringPrefix + "-last-upload-state", "Last upload to S3 Archive");
        this.lastUploadState.updateValue(INFO, "Nothing uploaded yet");
    }

    @Override
    public void apply(@Nonnull Position position, @Nonnull Event deserializedEvent) {
        if (deserializedEvent instanceof S3Archiver.EventRecordHolder) {
            currentBatchWriter.add(new ResolvedEvent(position, ((S3Archiver.EventRecordHolder) deserializedEvent).record));

            if (currentBatchWriter.readyToUpload()) {
                String key = currentBatchWriter.key();
                try {
                    S3BatchObject s3BatchObject = currentBatchWriter.prepareBatchForUpload();

                    try (Timer.Context ignored = s3UploadTimer.time()) {
                        Map<String, String> allMetadata = new HashMap<>(appMetadata);
                        allMetadata.putAll(s3BatchObject.metadata);
                        output.upload(key, s3BatchObject.content, s3BatchObject.contentLength, allMetadata);
                    }
                    lastUploadState.updateValue(INFO, format("Successfully uploaded object=[%s] at [%s]", key, clock.instant()));

                    currentBatchWriter.reset();
                } catch (IOException e) {
                    lastUploadState.updateValue(INFO, format("Failed to upload object=[%s] at [%s]%n%s", key, clock.instant(), ComponentUtils.getStackTraceAsString(e)));

                    throw new RuntimeException(
                            format("Error uploading object with key=[%s]%nThrowing exception to halt subscription, and dropping current batch.", key),
                            e);
                }
            }

            eventsAwaitingUploadComponent.updateValue(INFO, currentBatchWriter.eventsInCurrentBatch());
        }
    }

    @SuppressWarnings("WeakerAccess")
    public Collection<Component> monitoring() {
        return Arrays.asList(eventsAwaitingUploadComponent, lastUploadState);
    }

}
