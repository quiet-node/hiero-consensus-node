// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.event.preconsensus;

import static org.apache.logging.log4j.Level.CATEGORY;

import com.swirlds.base.time.Time;
import com.swirlds.common.context.PlatformContext;
import com.swirlds.common.metrics.IntegerPairAccumulator;
import com.swirlds.metrics.api.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import org.hiero.consensus.model.event.PlatformEvent;
import org.hiero.consensus.model.hashgraph.EventWindow;
import org.hiero.consensus.model.node.NodeId;

public class DefaultInlinePcesWriter implements InlinePcesWriter {

    public static final IntegerPairAccumulator.Config<Double> PCES_AVG_EVENT_SIZE = new IntegerPairAccumulator.Config<>(
                    CATEGORY, "pcesAvgEventSize", Double.class, DefaultInlinePcesWriter::average)
            .withDescription("The average length in bytes of an event written in a pces file");
    public static final IntegerPairAccumulator.Config<Double> PCES_AVG_SYNC_DURATION =
            new IntegerPairAccumulator.Config<>(
                            CATEGORY, "pcesAvgSyncDuration", Double.class, DefaultInlinePcesWriter::average)
                    .withDescription("The amount of time it takes to complete a flush operation");
    public static final IntegerPairAccumulator.Config<Double> PCES_AVG_WRITE_DURATION =
            new IntegerPairAccumulator.Config<>(
                            CATEGORY, "pcesAvgWriteDuration", Double.class, DefaultInlinePcesWriter::average)
                    .withDescription("The amount of time it takes to complete a single write operation");
    public static final IntegerPairAccumulator.Config<Double> PCES_AVG_TOTAL_WRITE_DURATION =
            new IntegerPairAccumulator.Config<>(
                            CATEGORY, "pcesAvgTotalWriteDuration", Double.class, DefaultInlinePcesWriter::average)
                    .withDescription("The amount of time it takes to write a single event to the stream");

    private final CommonPcesWriter commonPcesWriter;
    private final NodeId selfId;
    private final FileSyncOption fileSyncOption;
    final IntegerPairAccumulator<Double> avgWriteMetric;
    final IntegerPairAccumulator<Double> avgSyncMetric;
    final IntegerPairAccumulator<Double> avgTotalWrite;
    final IntegerPairAccumulator<Double> avgEventSizeMetric;
    private final Time time;

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
        this.fileSyncOption = platformContext
                .getConfiguration()
                .getConfigData(PcesConfig.class)
                .inlinePcesSyncOption();

        final Metrics metrics = platformContext.getMetrics();
        this.time = platformContext.getTime();
        this.avgWriteMetric = metrics.getOrCreate(PCES_AVG_WRITE_DURATION);
        this.avgSyncMetric = metrics.getOrCreate(PCES_AVG_SYNC_DURATION);
        this.avgTotalWrite = metrics.getOrCreate(PCES_AVG_TOTAL_WRITE_DURATION);
        this.avgEventSizeMetric = metrics.getOrCreate(PCES_AVG_EVENT_SIZE);
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
    public PlatformEvent writeEvent(@NonNull final PlatformEvent event) {
        final long eventWriteOp = time.nanoTime();

        // if we aren't streaming new events yet, assume that the given event is already durable
        if (!commonPcesWriter.isStreamingNewEvents()) {
            return event;
        }

        if (commonPcesWriter.getFileType().selectIndicator(event) < commonPcesWriter.getNonAncientBoundary()) {
            // don't do anything with ancient events
            return event;
        }

        try {
            commonPcesWriter.prepareOutputStream(event);
            final long writeOp = time.nanoTime();
            final long size = commonPcesWriter.getCurrentMutableFile().writeEvent(event);

            avgEventSizeMetric.update(size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size, 1);
            avgWriteMetric.update((int) (time.nanoTime() - writeOp), 1);

            if (fileSyncOption == FileSyncOption.EVERY_EVENT
                    || (fileSyncOption == FileSyncOption.EVERY_SELF_EVENT
                            && event.getCreatorId().equals(selfId))) {
                final long syncOp = time.nanoTime();
                commonPcesWriter.getCurrentMutableFile().sync();
                avgSyncMetric.update((int) (time.nanoTime() - syncOp), 1);
            }
            return event;
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            this.avgTotalWrite.update((int) (time.nanoTime() - eventWriteOp), 1);
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

    private static double average(int a, int b) {
        return b == 0 ? 0 : ((double) a / b);
    }
}
