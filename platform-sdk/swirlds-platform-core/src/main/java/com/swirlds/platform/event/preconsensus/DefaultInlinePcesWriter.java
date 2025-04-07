// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.event.preconsensus;

import static com.swirlds.metrics.api.FloatFormats.FORMAT_6_2;
import static com.swirlds.metrics.api.FloatFormats.FORMAT_9_6;

import com.swirlds.common.context.PlatformContext;
import com.swirlds.common.metrics.DurationGauge;
import com.swirlds.common.metrics.RunningAverageMetric;
import com.swirlds.common.metrics.SpeedometerMetric;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.Counter.Config;
import com.swirlds.metrics.api.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import org.hiero.consensus.model.event.PlatformEvent;
import org.hiero.consensus.model.hashgraph.EventWindow;
import org.hiero.consensus.model.node.NodeId;

public class DefaultInlinePcesWriter implements InlinePcesWriter {
    private final CommonPcesWriter commonPcesWriter;
    private final NodeId selfId;
    private final FileSyncOption fileSyncOption;
    private final Metrics metrics;
    // events_written_total: Counter for the number of events written.
    private static final Counter.Config events_written_total =
            new Config("platform", "DefaultInlinePcesWriter_events_written_total");
    // events_written_per_second: Counter for the number of events written.
    private static final DurationGauge.Config events_written_time =
            new DurationGauge.Config("platform", "DefaultInlinePcesWriter_events_written_time", ChronoUnit.NANOS);
    private static final DurationGauge.Config events_sync_time =
            new DurationGauge.Config("platform", "DefaultInlinePcesWriter_events_sync_time", ChronoUnit.NANOS);
    private static final SpeedometerMetric.Config events_written_per_second = new SpeedometerMetric.Config(
                    "platform", "DefaultInlinePcesWriter_events_written_per_second")
            .withDescription("number of events have been written per second")
            .withFormat(FORMAT_9_6);

    private static final Counter.Config event_bytes_written_total =
            new Config("platform", "DefaultInlinePcesWriter_event_bytes_written_total");
    // Write Latency
    // event_write_duration_seconds: Timer/histogram of how long it takes to write an event.
    private static final DurationGauge.Config event_write_duration_nanos = new DurationGauge.Config(
                    "platform", "DefaultInlinePcesWriter_event_write_duration_nanos", ChronoUnit.NANOS)
            .withDescription("how long it takes to write an event");
    // average_write_latency: Gauges for average write times (useful if not using histogram).
    private static final RunningAverageMetric.Config average_write_latency = new RunningAverageMetric.Config(
                    "platform", "DefaultInlinePcesWriter_average_write_latency")
            .withDescription("Gauges for average write times")
            .withFormat(FORMAT_6_2);
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
        final PcesConfig configData = platformContext.getConfiguration().getConfigData(PcesConfig.class);
        this.commonPcesWriter = new CommonPcesWriter(platformContext, fileManager);
        this.selfId = Objects.requireNonNull(selfId, "selfId is required");
        this.fileSyncOption = configData.inlinePcesSyncOption();
        this.metrics = platformContext.getMetrics();
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

        // if we aren't streaming new events yet, assume that the given event is already durable
        if (!commonPcesWriter.isStreamingNewEvents()) {
            return event;
        }

        if (event.getAncientIndicator(commonPcesWriter.getFileType()) < commonPcesWriter.getNonAncientBoundary()) {
            // don't do anything with ancient events
            return event;
        }

        try {
            long time = System.nanoTime();
            commonPcesWriter.prepareOutputStream(event);
            final var value = commonPcesWriter.getCurrentMutableFile().writeEvent(event);
            metrics.getOrCreate(event_write_duration_nanos).set(Duration.ofNanos(System.nanoTime() - time));
            metrics.getOrCreate(event_bytes_written_total).add(value);

            if (fileSyncOption == FileSyncOption.EVERY_EVENT
                    || (fileSyncOption == FileSyncOption.EVERY_SELF_EVENT
                            && event.getCreatorId().equals(selfId))) {

                long sync_time = System.nanoTime();
                commonPcesWriter.getCurrentMutableFile().sync();
                metrics.getOrCreate(events_sync_time).set(Duration.ofNanos(System.nanoTime() - sync_time));
            }
            long endTime = System.nanoTime();
            metrics.getOrCreate(events_written_total).increment();
            metrics.getOrCreate(events_written_per_second).cycle();
            metrics.getOrCreate(events_written_time).set(Duration.ofNanos(endTime - time));
            metrics.getOrCreate(average_write_latency).update(endTime - time);

            return event;
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
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
