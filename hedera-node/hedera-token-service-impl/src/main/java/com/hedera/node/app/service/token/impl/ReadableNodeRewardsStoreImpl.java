// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.service.token.impl;

import static com.hedera.node.app.service.token.impl.schemas.V0610TokenSchema.NODE_REWARDS_KEY;
import static java.util.Objects.requireNonNull;

import com.hedera.hapi.node.state.roster.RosterEntry;
import com.hedera.hapi.node.state.token.NodeActivity;
import com.hedera.hapi.node.state.token.NodeRewards;
import com.hedera.node.app.service.token.ReadableNetworkStakingRewardsStore;
import com.hedera.node.app.service.token.ReadableNodeRewardsStore;
import com.swirlds.state.spi.ReadableSingletonState;
import com.swirlds.state.spi.ReadableStates;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link ReadableNetworkStakingRewardsStore}.
 */
public class ReadableNodeRewardsStoreImpl implements ReadableNodeRewardsStore {

    /**
     * The underlying data storage class that holds staking reward data for all nodes.
     */
    private final ReadableSingletonState<NodeRewards> nodeRewardsState;

    /**
     * Create a new {@link ReadableNodeRewardsStoreImpl} instance.
     *
     * @param states The state to use.
     */
    public ReadableNodeRewardsStoreImpl(@NonNull final ReadableStates states) {
        this.nodeRewardsState = requireNonNull(states).getSingleton(NODE_REWARDS_KEY);
    }

    @Override
    public NodeRewards get() {
        return requireNonNull(nodeRewardsState.get());
    }

    public long numRoundsInStakingPeriod() {
        return requireNonNull(nodeRewardsState.get()).numRoundsInStakingPeriod();
    }

    /**
     * Returns the list of active node ids based on the active percent. A node is considered active if it has missed
     * less than the active percent of the total number of rounds in the staking period.
     * @param rosterEntries The list of roster entries.
     * @param activePercent The active percent.
     * @return The list of active node ids.
     */
    public List<Long> getActiveNodeIds(final List<RosterEntry> rosterEntries, final int activePercent) {
        final var nodeActivities = get().nodeActivities().stream()
                .collect(Collectors.toMap(NodeActivity::nodeId, NodeActivity::numMissedJudgeRounds));

        return rosterEntries.stream()
                .map(RosterEntry::nodeId)
                .filter(nodeId -> !nodeActivities.containsKey(nodeId)
                        || isActive(nodeActivities.get(nodeId), activePercent, numRoundsInStakingPeriod()))
                .toList();
    }

    private boolean isActive(
            final long numRoundsMissedNodeJudges, final int activePercent, final long numRoundsInStakingPeriod) {
        return numRoundsMissedNodeJudges <= numRoundsInStakingPeriod * activePercent / 100;
    }
}
