// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.event.preconsensus;

import com.swirlds.base.time.Time;
import com.swirlds.metrics.api.Metrics;
import com.swirlds.platform.stats.AtomicAverage;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;

/**
 * Used by {@link PcesFileWriter} to keeps local stats during the writing process.
 */
public class PcesFileEventStats {
    private final AtomicAverage averageEventSize = new AtomicAverage();
    private final AtomicAverage avgWriteDuration = new AtomicAverage();
    private final AtomicAverage avgSyncDuration = new AtomicAverage();
    private final AtomicAverage avgWriteEventDuration = new AtomicAverage();
    private final Time time;

    PcesFileEventStats(@NonNull final Time time) {
        this.time = Objects.requireNonNull(time);
    }

    /**
     * Updates the event size in bytes
     * @param size the event size in bytes
     */
    void updateEventSize(final long size) {
        if (size > 0) averageEventSize.update(size);
    }

    /**
     * returns a tracker for the average duration of a pcesFile write operation
     */
    @NonNull
    OpDurationTracker avgWriteDuration() {
        return new OpDurationTracker(time, avgWriteDuration);
    }

    /**
     * returns a tracker for the average duration of the time it takes for an event to reach to disk.
     * Includes sync and other metadata processing.
     */
    @NonNull
    OpDurationTracker avgWriteEventDuration() {
        return new OpDurationTracker(time, avgWriteEventDuration);
    }

    /**
     * returns a tracker for the average duration of a pcesFile sync operation
     */
    @NonNull
    OpDurationTracker avgSyncDuration() {
        return new OpDurationTracker(time, avgSyncDuration);
    }

    /**
     * Allows to track the duration of an operation
     */
    static class OpDurationTracker {

        private final Time time;
        private final long start;
        private final AtomicAverage stat;

        private OpDurationTracker(final Time time, AtomicAverage stat) {
            this.time = time;
            this.start = time.nanoTime();
            this.stat = stat;
        }

        /**
         * records the duration since the creation of the tracker and registers the value.
         */
        void end() {
            final long end = time.nanoTime();
            stat.update(end - start);
        }
    }

    /**
     * Updates the metrics with the stats reported by the writer
     */
    public void registerMetrics(@NonNull final Metrics metrics) {
        metrics.addUpdater(() -> {
            metrics.getOrCreate(PcesMetrics.PCES_AVG_WRITE_DURATION).update(avgWriteDuration.get());
            metrics.getOrCreate(PcesMetrics.PCES_AVG_SYNC_DURATION).update(avgSyncDuration.get());
            metrics.getOrCreate(PcesMetrics.PCES_AVG_TOTAL_WRITE_DURATION).update(avgWriteEventDuration.get());
            metrics.getOrCreate(PcesMetrics.AVG_EVENT_SIZE).update(averageEventSize.get());
        });
    }
}
