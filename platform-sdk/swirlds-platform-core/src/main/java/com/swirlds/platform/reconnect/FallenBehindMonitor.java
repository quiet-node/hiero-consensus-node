// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.reconnect;

import static com.swirlds.metrics.api.Metrics.INTERNAL_CATEGORY;

import com.hedera.hapi.node.state.roster.Roster;
import com.swirlds.base.state.Startable;
import com.swirlds.common.merkle.synchronization.config.ReconnectConfig;
import com.swirlds.common.metrics.FunctionGauge;
import com.swirlds.config.api.Configuration;
import com.swirlds.metrics.api.Metrics;
import com.swirlds.platform.system.status.StatusActionSubmitter;
import com.swirlds.platform.system.status.actions.FallenBehindAction;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.hiero.consensus.model.node.NodeId;

/**
 */
public class FallenBehindMonitor {

    /**
     * the number of neighbors we have
     */
    private final int numNeighbors;

    /**
     * set of neighbors who report that this node has fallen behind
     */
    private final Set<NodeId> reportFallenBehind = new HashSet<>();

    /**
     * Enables submitting platform status actions
     */
    private StatusActionSubmitter statusActionSubmitter;

    private final ReconnectConfig config;
    private boolean previouslyFallenBehind;
    private Startable reconnectStarter;

    public FallenBehindMonitor(
            final Roster roster, @NonNull final Configuration config, @NonNull final Metrics metrics) {
        this.numNeighbors = roster.rosterEntries().size();

        this.config = Objects.requireNonNull(config, "config must not be null").getConfigData(ReconnectConfig.class);

        metrics.getOrCreate(
                new FunctionGauge.Config<>(INTERNAL_CATEGORY, "hasFallenBehind", Object.class, this::hasFallenBehind)
                        .withDescription("has this node fallen behind?"));
        metrics.getOrCreate(new FunctionGauge.Config<>(
                        INTERNAL_CATEGORY, "numReportFallenBehind", Integer.class, this::reportedSize)
                .withDescription("the number of nodes that have fallen behind")
                .withUnit("count"));
    }

    /**
     * Notify the fallen behind manager that a node has reported that node is providing us with events we need. This
     * means we are not in fallen behind state against that node.
     *
     * @param id the id of the node who is providing us with up to date events
     */
    public synchronized void clear(@NonNull final NodeId id) {
        reportFallenBehind.remove(id);
    }

    /**
     * Notify the fallen behind manager that a node has reported that they don't have events we need. This means we have
     * probably fallen behind and will need to reconnect
     *
     * @param id the id of the node who says we have fallen behind
     */
    public synchronized void report(@NonNull final NodeId id) {
        if (reportFallenBehind.add(id)) {
            if (!previouslyFallenBehind && hasFallenBehind()) {
                statusActionSubmitter.submitStatusAction(new FallenBehindAction());
                previouslyFallenBehind = true;
                reconnectStarter.start();
            }
        }
    }

    /**
     * Have enough nodes reported that they don't have events we need, and that we have fallen behind?
     *
     * @return true if we have fallen behind, false otherwise
     */
    public synchronized boolean hasFallenBehind() {
        return numNeighbors * config.fallenBehindThreshold() < reportFallenBehind.size();
    }

    /**
     * Should I attempt a reconnect with this neighbor?
     *
     * @param peerId the ID of the neighbor
     * @return true if I should attempt a reconnect
     */
    public boolean hasPeerReported(@NonNull final NodeId peerId) {
        if (!hasFallenBehind()) {
            return false;
        }
        synchronized (this) {
            // if this neighbor has told me I have fallen behind, I will reconnect with him
            return reportFallenBehind.contains(peerId);
        }
    }

    /**
     * We have determined that we have not fallen behind, or we have reconnected, so reset everything to the initial
     * state
     */
    public synchronized void reset() {
        reportFallenBehind.clear();
        previouslyFallenBehind = false;
    }

    /**
     * @return the number of nodes that have told us we have fallen behind
     */
    public synchronized int reportedSize() {
        return reportFallenBehind.size();
    }

    public void bind(Startable reconnectStarter) {
        this.reconnectStarter = reconnectStarter;
    }

    public void bind(StatusActionSubmitter statusActionSubmitter) {
        this.statusActionSubmitter = Objects.requireNonNull(statusActionSubmitter);
    }
}
