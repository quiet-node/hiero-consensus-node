// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.event.preconsensus;

import com.swirlds.base.time.Time;
import com.swirlds.common.context.PlatformContext;
import com.swirlds.common.metrics.RunningAverageMetric;
import com.swirlds.metrics.api.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import org.hiero.consensus.model.event.PlatformEvent;
import org.hiero.consensus.model.hashgraph.EventWindow;
import org.hiero.consensus.model.node.NodeId;

public class DefaultInlinePcesWriter implements InlinePcesWriter {
    private final CommonPcesWriter commonPcesWriter;
    private final NodeId selfId;
    private final FileSyncOption fileSyncOption;
    private final Time time;
    private final RunningAverageMetric avgWriteDuration;
    private final RunningAverageMetric avgSyncDuration;
    private final RunningAverageMetric avgTotalWriteDuration;
    /**
     * Constructor
     *
     * @param platformContext the platform context
     * @param fileManager     manages all preconsensus event stream files currently on disk
     */
    public DefaultInlinePcesWriter(
            @NonNull final PlatformContext platformContext,
            @NonNull final PcesFileManager fileManager,
            @NonNull final NodeId selfId) {
        Objects.requireNonNull(platformContext, "platformContext is required");
        Objects.requireNonNull(fileManager, "fileManager is required");
        this.commonPcesWriter = new CommonPcesWriter(platformContext, fileManager);
        this.selfId = Objects.requireNonNull(selfId, "selfId is required");
        this.time = platformContext.getTime();
        this.fileSyncOption = platformContext
                .getConfiguration()
                .getConfigData(PcesConfig.class)
                .inlinePcesSyncOption();
        final Metrics metrics = platformContext.getMetrics();
        this.avgWriteDuration = metrics.getOrCreate(PcesMetrics.PCES_AVG_WRITE_DURATION);
        this.avgSyncDuration = metrics.getOrCreate(PcesMetrics.PCES_AVG_SYNC_DURATION);
        this.avgTotalWriteDuration = metrics.getOrCreate(PcesMetrics.PCES_AVG_TOTAL_WRITE_DURATION);
    }

    @Override
    public void beginStreamingNewEvents() {
        commonPcesWriter.beginStreamingNewEvents();
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public PlatformEvent writeEvent(@NonNull PlatformEvent event) {
        long nanoStartTime = System.nanoTime();
        // if we aren't streaming new events yet, assume that the given event is already durable
        if (!commonPcesWriter.isStreamingNewEvents()) {
            return event;
        }

        if (commonPcesWriter.getFileType().selectIndicator(event) < commonPcesWriter.getNonAncientBoundary()) {
            // don't do anything with ancient events
            return event;
        }

        long nanoWriteStartTime = nanoStartTime;
        long nanoWriteEndTime = nanoStartTime;
        long nanoSyncEndTime = nanoStartTime;
        try {
            commonPcesWriter.prepareOutputStream(event);
            nanoWriteStartTime = time.nanoTime();
            commonPcesWriter.getCurrentMutableFile().writeEvent(event);
            nanoWriteEndTime = time.nanoTime();
            nanoSyncEndTime = nanoWriteEndTime;
            if (fileSyncOption == FileSyncOption.EVERY_EVENT
                    || (fileSyncOption == FileSyncOption.EVERY_SELF_EVENT
                            && event.getCreatorId().equals(selfId))) {

                commonPcesWriter.getCurrentMutableFile().sync();
                nanoSyncEndTime = time.nanoTime();
            }
            return event;
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            avgWriteDuration.update(nanoWriteEndTime - nanoWriteStartTime);
            avgSyncDuration.update(nanoSyncEndTime - nanoWriteEndTime);
            avgTotalWriteDuration.update(time.nanoTime() - nanoStartTime);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerDiscontinuity(@NonNull Long newOriginRound) {
        commonPcesWriter.registerDiscontinuity(newOriginRound);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateNonAncientEventBoundary(@NonNull EventWindow nonAncientBoundary) {
        commonPcesWriter.updateNonAncientEventBoundary(nonAncientBoundary);
    }

    @Override
    public void setMinimumAncientIdentifierToStore(@NonNull final Long minimumAncientIdentifierToStore) {
        commonPcesWriter.setMinimumAncientIdentifierToStore(minimumAncientIdentifierToStore);
    }
}
