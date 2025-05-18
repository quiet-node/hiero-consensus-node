// SPDX-License-Identifier: Apache-2.0
package org.hiero.otter.fixtures.metric;

// SPDX-License-Identifier: Apache-2.0

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.hiero.consensus.model.node.NodeId;

/**
 * Central singleton that collects and manages multiple {@link NodeMetrics} instances.
 * <p>
 * Every node that is instantiated inside an otter test should create a {@link NodeMetrics}
 * instance which will automatically register itself with this singleton. The collected
 * instances can then be queried in assertions to validate metric behaviour across the whole
 * test network.
 */
public final class NetworkMetrics {

    private static final NetworkMetrics INSTANCE = new NetworkMetrics();

    /** Mapping nodeId -> metrics instance */
    private final Map<NodeId, NodeMetrics> nodeMetricsMap = new ConcurrentHashMap<>();

    private NetworkMetrics() {
        // singleton
    }

    /**
     * Get the singleton instance.
     *
     * @return the instance
     */
    @NonNull
    public static NetworkMetrics getInstance() {
        return INSTANCE;
    }

    /**
     * Register a {@link NodeMetrics} instance. If an instance for the same nodeId was already present, it will be
     * replaced.
     *
     * @param nodeMetrics the instance to register
     */
    public void register(@NonNull final NodeMetrics nodeMetrics) {
        Objects.requireNonNull(nodeMetrics, "nodeMetrics cannot be null");
        nodeMetricsMap.put(nodeMetrics.getNodeId(), nodeMetrics);
    }

    public void unregister(@NonNull final NodeId nodeId) {
        Objects.requireNonNull(nodeId, "nodeId cannot be null");
        nodeMetricsMap.remove(nodeId);
    }
    /**
     * Get the metrics for a single node.
     *
     * @param nodeId the id of the node
     * @return the metrics or {@code null} if no metrics have been registered for this node
     */
    @NonNull
    public NodeMetrics get(@NonNull final NodeId nodeId) {
        Objects.requireNonNull(nodeId, "nodeId cannot be null");
        return nodeMetricsMap.get(nodeId);
    }

    /**
     * Get an immutable view of all known node metrics.
     *
     * @return collection of all node metrics
     */
    @NonNull
    public Collection<NodeMetrics> getAll() {
        return Collections.unmodifiableCollection(nodeMetricsMap.values());
    }
}
