// SPDX-License-Identifier: Apache-2.0
package org.hiero.otter.fixtures.result;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import org.hiero.consensus.model.node.NodeId;
import org.hiero.otter.fixtures.turtle.metric.MetricsCollector.NumberValue;

/**
 * Interface that provides the metrics results for a single node.
 *
 * This interface allows retrieval of the unique identifier of the node,
 * the metric's identifier, and the historical records of the metric values.
 */
public interface SingleNodeMetricsResult {
    /**
     * Retrieves the unique identifier of the node.
     *
     * @return the {@link NodeId} representing the unique identifier of the node
     */
    @NonNull
    NodeId nodeId();

    /**
     * Retrieves the identifier of the metric associated with the current node.
     *
     * @return the metric's identifier as a string
     */
    @NonNull
    String identifier();

    /**
     * Retrieves a list of historical metric values for the associated node.
     * Each value includes a timestamp and the corresponding numeric value of the metric.
     *
     * @return a list of {@link NumberValue} objects representing the historical metric data
     */
    @NonNull
    List<NumberValue> history();
}
