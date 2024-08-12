package com.timgroup.eventstore.archiver;

import com.codahale.metrics.Timer;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.archiver.monitoring.ComponentUtils;
import com.timgroup.eventsubscription.Event;
import com.timgroup.eventsubscription.EventHandler;
import com.timgroup.remotefilestorage.s3.UploadStorage;
import com.timgroup.tucker.info.Component;
import com.timgroup.tucker.info.component.SimpleValueComponent;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.timgroup.tucker.info.Status.INFO;
import static com.timgroup.tucker.info.Status.WARNING;
import static java.lang.String.format;

final class BatchingUploadHandler implements EventHandler {
    private final UploadStorage output;
    private final Clock clock;
    private final CurrentBatchWriter currentBatchWriter;

    private final Map<String, String> appMetadata;
    private final Timer s3UploadTimer;
    private final SimpleValueComponent eventsAwaitingUploadComponent;
    private final long uploadRetryDelay;
    private final SimpleValueComponent lastUploadState;

    BatchingUploadHandler(
            UploadStorage uploadableStorage,
            CurrentBatchWriter currentBatchWriter,
            Clock clock,
            Map<String, String> appMetadata,
            String monitoringPrefix,
            Timer s3UploadTimer,
            long uploadRetryDelay)
    {
        this.output = uploadableStorage;
        this.clock = clock;
        this.currentBatchWriter = currentBatchWriter;

        this.appMetadata = appMetadata;
        this.s3UploadTimer = s3UploadTimer;
        this.eventsAwaitingUploadComponent = new SimpleValueComponent(monitoringPrefix + "-events-awaiting-upload", "Number of events awaiting upload to archive");
        this.uploadRetryDelay = uploadRetryDelay;
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

                    upload(s3BatchObject, key);

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

    private void upload(S3BatchObject s3BatchObject, String key) {
        while (true) {
            try (Timer.Context ignored = s3UploadTimer.time()) {
                Map<String, String> allMetadata = new HashMap<>(appMetadata);
                allMetadata.putAll(s3BatchObject.metadata);
                output.upload(key,
                        new ByteArrayInputStream(s3BatchObject.content),
                        s3BatchObject.contentLength,
                        allMetadata);
                break;
            } catch (Exception e) {
                lastUploadState.updateValue(WARNING, format("Failed to upload object=[%s] at [%s]%n%s. Retrying.", key,
                        clock.instant(), ComponentUtils.getStackTraceAsString(e)));

                try {
                    Thread.sleep(uploadRetryDelay);
                } catch (InterruptedException ex) {}
            }
        }

        lastUploadState.updateValue(INFO, format("Successfully uploaded object=[%s] at [%s]", key, clock.instant()));
    }

    @SuppressWarnings("WeakerAccess")
    public Collection<Component> monitoring() {
        return Arrays.asList(eventsAwaitingUploadComponent, lastUploadState);
    }

}
