// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.event.preconsensus;

import static com.swirlds.common.metrics.IntegerPairAccumulator.AVERAGE;
import static org.apache.logging.log4j.Level.CATEGORY;

import com.swirlds.base.time.Time;
import com.swirlds.common.metrics.IntegerPairAccumulator;
import com.swirlds.metrics.api.Metrics;
import javax.annotation.Nonnull;

/**
 * Used by {@link DefaultInlinePcesWriter} to keep track of the write and sync duration.
 * This class is not threadsafe.
 */
public class PcesWriterPerEventMetrics {

    private static final IntegerPairAccumulator.Config<Double> PCES_AVG_EVENT_SIZE =
            new IntegerPairAccumulator.Config<>(CATEGORY, "pcesAvgEventSize", Double.class, AVERAGE)
                    .withDescription("The average length in bytes of an event written in a pces file");
    private static final IntegerPairAccumulator.Config<Double> PCES_AVG_SYNC_DURATION =
            new IntegerPairAccumulator.Config<>(CATEGORY, "pcesAvgSyncDuration", Double.class, AVERAGE)
                    .withDescription("The amount of time it takes to complete a flush operation");
    private static final IntegerPairAccumulator.Config<Double> PCES_AVG_WRITE_DURATION =
            new IntegerPairAccumulator.Config<>(CATEGORY, "pcesAvgWriteDuration", Double.class, AVERAGE)
                    .withDescription("The amount of time it takes to complete a single write operation");
    private static final IntegerPairAccumulator.Config<Double> PCES_AVG_TOTAL_WRITE_DURATION =
            new IntegerPairAccumulator.Config<>(CATEGORY, "pcesAvgTotalWriteDuration", Double.class, AVERAGE)
                    .withDescription("The amount of time it takes to write a single event to the stream");

    private final IntegerPairAccumulator<Double> avgWriteMetric;
    private final IntegerPairAccumulator<Double> avgSyncMetric;
    private final IntegerPairAccumulator<Double> avgTotalWrite;
    private final IntegerPairAccumulator<Double> avgEventSizeMetric;
    private final Time time;

    private long totalWriteStart;
    private long writeStart;
    private long syncStart;

    public PcesWriterPerEventMetrics(@Nonnull final Metrics metrics, @Nonnull final Time time) {
        this.avgWriteMetric = metrics.getOrCreate(PCES_AVG_WRITE_DURATION);
        this.avgSyncMetric = metrics.getOrCreate(PCES_AVG_SYNC_DURATION);
        this.avgTotalWrite = metrics.getOrCreate(PCES_AVG_TOTAL_WRITE_DURATION);
        this.avgEventSizeMetric = metrics.getOrCreate(PCES_AVG_EVENT_SIZE);
        this.time = time;
    }

    /**
     * marks the start of the write operation.
     */
    void startWriteEvent() {
        this.totalWriteStart = time.nanoTime();
    }

    /**
     * marks the start of the sync operation.
     */
    void startSyncToFile() {
        this.syncStart = time.nanoTime();
    }

    /**
     * marks the start of the write operation.
     */
    void startWriteToFile() {
        this.writeStart = time.nanoTime();
    }

    /**
     * reports the duration of the write operation
     */
    void endWriteToFile(long size) {
        avgWriteMetric.update(getDurationInNanos(writeStart), 1);
        this.avgEventSizeMetric.update(asInt(size), 1);
    }

    /**
     * reports the duration of the total write operation
     */
    void endWriteEvent() {
        avgTotalWrite.update(getDurationInNanos(totalWriteStart), 1);
    }

    /**
     * reports the duration of the sync operation
     */
    void endSyncToFile() {
        avgSyncMetric.update(getDurationInNanos(syncStart), 1);
    }

    /**
     * Returns the duration in nanos from the given start up to now as reported by time instance
     * @param startNanos the start of the duration to consider
     * @return the value in nanos of the duration, up to Integer.MAX_VALUE
     */
    private int getDurationInNanos(final long startNanos) {
        final long val = time.nanoTime() - startNanos;
        return asInt(val);
    }

    /**
     * Returns the value of a long if is lower than Integer.MAX_VALUE or Integer.MAX_VALUE otherwise
     * @param val the value to check
     * @return the value of a long if is lower than Integer.MAX_VALUE or Integer.MAX_VALUE otherwise
     */
    private static int asInt(final long val) {
        return val > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) val;
    }

    /**
     * Clears the previously recorded start times
     */
    public void clear() {
        this.totalWriteStart = 0;
        this.writeStart = 0;
        this.syncStart = 0;
    }
}
