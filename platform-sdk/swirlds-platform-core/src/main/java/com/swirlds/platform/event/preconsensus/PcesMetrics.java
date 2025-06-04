// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.event.preconsensus;

import com.swirlds.common.metrics.RunningAverageMetric;
import com.swirlds.common.metrics.SpeedometerMetric;
import com.swirlds.metrics.api.DoubleGauge;
import com.swirlds.metrics.api.LongGauge;
import com.swirlds.metrics.api.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;

/**
 * Metrics for preconsensus events.
 */
public class PcesMetrics {

    private static final String CATEGORY = "platform";
    private final Metrics metrics;

    private static final LongGauge.Config PRECONSENSUS_EVENT_FILE_COUNT_CONFIG = new LongGauge.Config(
                    CATEGORY, "preconsensusEventFileCount")
            .withUnit("count")
            .withDescription("The number of preconsensus event files currently being stored");
    private final LongGauge preconsensusEventFileCount;

    private static final DoubleGauge.Config PRECONSENSUS_EVENT_FILE_AVERAGE_SIZE_MB_CONFIG = new DoubleGauge.Config(
                    CATEGORY, "preconsensusEventFileAverageSizeMB")
            .withUnit("megabytes")
            .withDescription("The average size of preconsensus event files, in megabytes.");
    private final DoubleGauge preconsensusEventFileAverageSizeMB;

    private static final DoubleGauge.Config PRECONSENSUS_EVENT_FILE_TOTAL_SIZE_GB_CONFIG = new DoubleGauge.Config(
                    CATEGORY, "preconsensusEventFileTotalSizeGB")
            .withUnit("gigabytes")
            .withDescription("The total size of all preconsensus event files, in gigabytes.");
    private final DoubleGauge preconsensusEventFileTotalSizeGB;

    private static final SpeedometerMetric.Config PRECONSENSUS_EVENT_FILE_RATE_CONFIG = new SpeedometerMetric.Config(
                    CATEGORY, "preconsensusEventFileRate")
            .withUnit("hertz")
            .withDescription("The number of preconsensus event files written per second.");
    private final SpeedometerMetric preconsensusEventFileRate;

    private static final RunningAverageMetric.Config PRECONSENSUS_EVENT_AVERAGE_FILE_SPAN_CONFIG =
            new RunningAverageMetric.Config(CATEGORY, "preconsensusEventAverageFileSpan")
                    .withDescription("The average span of preconsensus event files. Only reflects"
                            + "files written since the last restart.");
    private final RunningAverageMetric preconsensusEventAverageFileSpan;

    private static final RunningAverageMetric.Config PRECONSENSUS_EVENT_AVERAGE_UN_UTILIZED_FILE_SPAN_CONFIG =
            new RunningAverageMetric.Config(CATEGORY, "preconsensusEventAverageUnutilizedFileSpan")
                    .withDescription(
                            "The average unutilized span of preconsensus event files prior "
                                    + "to span compaction. Only reflects files written since the last restart. Smaller is better.");
    private final RunningAverageMetric preconsensusEventAverageUnUtilizedFileSpan;

    private static final LongGauge.Config PRECONSENSUS_EVENT_FILE_OLDEST_IDENTIFIER_CONFIG = new LongGauge.Config(
                    CATEGORY, "preconsensusEventFileOldestIdentifier")
            .withDescription("The oldest possible ancient indicator that is being stored in preconsensus event files.");
    private final LongGauge preconsensusEventFileOldestIdentifier;

    private static final LongGauge.Config PRECONSENSUS_EVENT_FILE_YOUNGEST_IDENTIFIER_CONFIG = new LongGauge.Config(
                    CATEGORY, "preconsensusEventFileYoungestIdentifier")
            .withDescription(
                    "The youngest possible ancient indicator that is being stored in preconsensus event files.");
    private final LongGauge preconsensusEventFileYoungestIdentifier;

    private static final LongGauge.Config PRECONSENSUS_EVENT_FILE_OLDEST_SECONDS_CONFIG = new LongGauge.Config(
                    CATEGORY, "preconsensusEventFileOldestSeconds")
            .withUnit("seconds")
            .withDescription("The age of the oldest preconsensus event file, in seconds.");
    private final LongGauge preconsensusEventFileOldestSeconds;

    public static final RunningAverageMetric.Config PCES_AVG_EVENT_SIZE = new RunningAverageMetric.Config(
                    CATEGORY, "pcesAvgEventSize")
            .withDescription("The average length in bytes of an event written in a pces file");
    public static final RunningAverageMetric.Config PCES_AVG_SYNC_DURATION = new RunningAverageMetric.Config(
                    CATEGORY, "pcesAvgSyncDuration")
            .withDescription("The amount of time it takes to complete a flush operation");
    public static final RunningAverageMetric.Config PCES_AVG_WRITE_DURATION = new RunningAverageMetric.Config(
                    CATEGORY, "pcesAvgWriteDuration")
            .withDescription("The amount of time it takes to complete a single write operation");
    public static final RunningAverageMetric.Config PCES_AVG_TOTAL_WRITE_DURATION = new RunningAverageMetric.Config(
                    CATEGORY, "pcesAvgTotalWriteDuration")
            .withDescription("The amount of time it takes to write a single event to the stream");

    /**
     * Construct preconsensus event metrics.
     *
     * @param metrics the metrics manager for the platform
     */
    public PcesMetrics(final @NonNull Metrics metrics) {
        this.metrics = Objects.requireNonNull(metrics);
        preconsensusEventFileCount = metrics.getOrCreate(PRECONSENSUS_EVENT_FILE_COUNT_CONFIG);
        preconsensusEventFileAverageSizeMB = metrics.getOrCreate(PRECONSENSUS_EVENT_FILE_AVERAGE_SIZE_MB_CONFIG);
        preconsensusEventFileTotalSizeGB = metrics.getOrCreate(PRECONSENSUS_EVENT_FILE_TOTAL_SIZE_GB_CONFIG);
        preconsensusEventFileRate = metrics.getOrCreate(PRECONSENSUS_EVENT_FILE_RATE_CONFIG);
        preconsensusEventAverageFileSpan = metrics.getOrCreate(PRECONSENSUS_EVENT_AVERAGE_FILE_SPAN_CONFIG);
        preconsensusEventAverageUnUtilizedFileSpan =
                metrics.getOrCreate(PRECONSENSUS_EVENT_AVERAGE_UN_UTILIZED_FILE_SPAN_CONFIG);
        preconsensusEventFileOldestIdentifier = metrics.getOrCreate(PRECONSENSUS_EVENT_FILE_OLDEST_IDENTIFIER_CONFIG);
        preconsensusEventFileYoungestIdentifier =
                metrics.getOrCreate(PRECONSENSUS_EVENT_FILE_YOUNGEST_IDENTIFIER_CONFIG);
        preconsensusEventFileOldestSeconds = metrics.getOrCreate(PRECONSENSUS_EVENT_FILE_OLDEST_SECONDS_CONFIG);

        // crating the metrics early so we know they can be exported to the csv
        metrics.getOrCreate(PcesMetrics.PCES_AVG_EVENT_SIZE);
        metrics.getOrCreate(PcesMetrics.PCES_AVG_SYNC_DURATION);
        metrics.getOrCreate(PcesMetrics.PCES_AVG_WRITE_DURATION);
        metrics.getOrCreate(PcesMetrics.PCES_AVG_TOTAL_WRITE_DURATION);
    }

    /**
     * Get the metric tracking the total number of preconsensus event files currently being tracked.
     */
    public LongGauge getPreconsensusEventFileCount() {
        return preconsensusEventFileCount;
    }

    /**
     * Get the metric tracking the average size of preconsensus event files.
     */
    public DoubleGauge getPreconsensusEventFileAverageSizeMB() {
        return preconsensusEventFileAverageSizeMB;
    }

    /**
     * Get the metric tracking the total size of all preconsensus event files.
     */
    public DoubleGauge getPreconsensusEventFileTotalSizeGB() {
        return preconsensusEventFileTotalSizeGB;
    }

    /**
     * Get the metric tracking the rate at which preconsensus event files are being created.
     */
    public SpeedometerMetric getPreconsensusEventFileRate() {
        return preconsensusEventFileRate;
    }

    /**
     * Get the metric tracking the average file span.
     */
    public RunningAverageMetric getPreconsensusEventAverageFileSpan() {
        return preconsensusEventAverageFileSpan;
    }

    /**
     * Get the metric tracking the average un-utilized file span.
     */
    public RunningAverageMetric getPreconsensusEventAverageUnUtilizedFileSpan() {
        return preconsensusEventAverageUnUtilizedFileSpan;
    }

    /**
     * Get the metric tracking the oldest possible ancient indicator that is being stored in preconsensus event files.
     */
    public LongGauge getPreconsensusEventFileOldestIdentifier() {
        return preconsensusEventFileOldestIdentifier;
    }

    /**
     * Get the metric tracking the youngest possible ancient indicator that is being stored in preconsensus event files.
     */
    public LongGauge getPreconsensusEventFileYoungestIdentifier() {
        return preconsensusEventFileYoungestIdentifier;
    }

    /**
     * Get the metric tracking the age of the oldest preconsensus event file, in seconds.
     */
    public LongGauge getPreconsensusEventFileOldestSeconds() {
        return preconsensusEventFileOldestSeconds;
    }
}
