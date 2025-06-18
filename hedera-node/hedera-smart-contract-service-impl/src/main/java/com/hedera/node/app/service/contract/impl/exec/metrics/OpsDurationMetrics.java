// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.service.contract.impl.exec.metrics;

import static java.util.Objects.requireNonNull;

import com.hedera.node.app.service.contract.impl.exec.utils.SystemContractMethod;
import com.swirlds.metrics.api.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import javax.inject.Inject;

public class OpsDurationMetrics {
    private final SystemContractOpsDurationMetric systemContractOpsDurationMetric;
    private final OpCodeOpsDurationMetric opCodeOpsDurationMetric;
    private final PrecompileOpsDurationMetric precompileOpsDurationMetric;
    private final OpsDurationPerTransactionMetrics opsDurationPerTransactionMetrics;
    private final TransactionThrottledByOpsDurationMetric transactionThrottledByOpsDurationMetric;

    @Inject
    public OpsDurationMetrics(@NonNull final Metrics metrics) {
        requireNonNull(metrics, "Metrics cannot be null");
        this.systemContractOpsDurationMetric = new SystemContractOpsDurationMetric(metrics);
        this.opCodeOpsDurationMetric = new OpCodeOpsDurationMetric(metrics);
        this.precompileOpsDurationMetric = new PrecompileOpsDurationMetric(metrics);
        this.opsDurationPerTransactionMetrics = new OpsDurationPerTransactionMetrics(metrics);
        this.transactionThrottledByOpsDurationMetric = new TransactionThrottledByOpsDurationMetric(metrics);
    }

    /**
     * Records the duration of a system contract operation in nanoseconds
     *
     * @param method the system contract method that was executed
     * @param durationNanos the duration in nanoseconds
     */
    public void recordSystemContractDuration(@NonNull final SystemContractMethod method, final long durationNanos) {
        systemContractOpsDurationMetric.recordOperationDuration(method, durationNanos);
    }

    /**
     * Gets the current average duration for a specific system contract operation
     *
     * @param method the system contract method to get duration for
     * @return the average duration in nanoseconds
     */
    public double getAverageSystemContractDuration(@NonNull final SystemContractMethod method) {
        return systemContractOpsDurationMetric.getAverageSystemContractOpsDuration(method);
    }

    /**
     * Records the duration of an EVM operation in nanoseconds
     *
     * @param opCode the EVM op code that was executed
     * @param durationNanos the duration in nanoseconds
     */
    public void recordOpCodeOpsDuration(@NonNull final int opCode, final long durationNanos) {
        opCodeOpsDurationMetric.recordOperationDuration(opCode, durationNanos);
    }

    /**
     * Gets the current average duration for a specific operation
     *
     * @param opCode the EVM op code to get duration for
     * @return the average duration in nanoseconds
     */
    public double getAverageEVMOperationDuration(@NonNull final int opCode) {
        return opCodeOpsDurationMetric.getAverageOperationDuration(opCode);
    }

    /**
     * Records the duration of an EVM operation in nanoseconds
     *
     * @param precompile the precompile op that was executed
     * @param durationNanos the duration in nanoseconds
     */
    public void recordPrecompileDuration(@NonNull final String precompile, final long durationNanos) {
        precompileOpsDurationMetric.recordPrecompileDuration(precompile, durationNanos);
    }

    /**
     * Gets the current average duration for a specific precompile
     *
     * @param precompile the precompile op to get duration for
     * @return the average duration in nanoseconds
     */
    public double getAveragePrecompileDuration(@NonNull final String precompile) {
        return precompileOpsDurationMetric.getAveragePrecompileDuration(precompile);
    }

    /**
     * Records the duration of a transaction in nanoseconds
     * @param opsDurationNanos the duration in nanoseconds
     */
    public void recordTxnTotalOpsDuration(final long opsDurationNanos) {
        opsDurationPerTransactionMetrics.recordTxnTotalOpsDuration(opsDurationNanos);
    }

    /**
     * Records the count of transactions that were throttled due to exceeding the maximum allowed
     */
    public void recordTxnThrottledByOpsDuration() {
        transactionThrottledByOpsDurationMetric.increment();
    }
}
