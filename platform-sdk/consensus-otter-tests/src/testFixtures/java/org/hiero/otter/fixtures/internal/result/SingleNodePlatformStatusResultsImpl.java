// SPDX-License-Identifier: Apache-2.0
package org.hiero.otter.fixtures.internal.result;

import static java.util.Objects.requireNonNull;

import com.hedera.hapi.platform.state.NodeId;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import org.hiero.consensus.model.status.PlatformStatus;
import org.hiero.otter.fixtures.result.PlatformStatusSubscriber;
import org.hiero.otter.fixtures.result.SingleNodePlatformStatusResults;

/**
 * Default implementation of {@link SingleNodePlatformStatusResults}
 */
public class SingleNodePlatformStatusResultsImpl implements SingleNodePlatformStatusResults {

    private final NodeResultsCollector collector;

    // This class may be used in a multi-threaded context, so we use volatile to ensure visibility of state changes
    private volatile int startIndex = 0;

    /**
     * Creates a new instance of {@link SingleNodePlatformStatusResultsImpl}.
     *
     * @param collector the {@link NodeResultsCollector} that collects the results
     */
    public SingleNodePlatformStatusResultsImpl(@NonNull final NodeResultsCollector collector) {
        this.collector = requireNonNull(collector);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public NodeId nodeId() {
        return collector.nodeId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public List<PlatformStatus> statusProgression() {
        return collector.currentStatusProgression(startIndex);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void subscribe(@NonNull final PlatformStatusSubscriber subscriber) {
        collector.subscribePlatformStatusSubscriber(subscriber);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        startIndex = collector.currentStatusProgression(0).size();
    }
}
