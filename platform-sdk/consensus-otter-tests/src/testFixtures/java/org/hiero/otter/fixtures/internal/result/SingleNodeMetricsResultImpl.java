// SPDX-License-Identifier: Apache-2.0
package org.hiero.otter.fixtures.internal.result;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Objects;
import org.hiero.consensus.model.node.NodeId;
import org.hiero.otter.fixtures.result.SingleNodeMetricsResult;
import org.hiero.otter.fixtures.turtle.metric.MetricsCollector.NumberValue;

/**
 * Implementation of {@link SingleNodeMetricsResult} that stores the metric results for a single node.
 *
 * @param nodeId the ID of the node
 * @param identifier identifier of the metric
 * @param history all stored values of the metric
 */
public record SingleNodeMetricsResultImpl(
        @NonNull NodeId nodeId, @NonNull String identifier, @NonNull List<NumberValue> history)
        implements SingleNodeMetricsResult {

    /**
     * Constructor
     *
     * @param nodeId the ID of the node
     * @param identifier identifier of the metric
     * @param history all stored values of the metric
     */
    public SingleNodeMetricsResultImpl {
        Objects.requireNonNull(nodeId, "nodeId cannot be null");
        Objects.requireNonNull(identifier, "identifier cannot be null");
        Objects.requireNonNull(history, "history cannot be null");
    }
}
