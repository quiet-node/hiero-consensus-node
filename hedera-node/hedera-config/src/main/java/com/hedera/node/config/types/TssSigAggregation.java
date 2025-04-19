// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.config.types;

/**
 * Enum representing the TSS signature aggregation strategy.
 */
public enum TssSigAggregation {
    /**
     * Nodes should aggregate TSS signatures in prehandle phase.
     */
    PREHANDLE,
    /**
     * Nodes should wait until consensus to aggregate TSS signatures.
     */
    CONSENSUS,
}
