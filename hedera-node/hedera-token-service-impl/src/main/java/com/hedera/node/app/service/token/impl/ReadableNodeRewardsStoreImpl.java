// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.service.token.impl;

import static com.hedera.node.app.service.token.impl.schemas.V0610TokenSchema.NODE_REWARDS_KEY;
import static java.util.Objects.requireNonNull;

import com.hedera.hapi.node.state.roster.RosterEntry;
import com.hedera.hapi.node.state.token.NodeActivity;
import com.hedera.hapi.node.state.token.NodeRewards;
import com.hedera.node.app.service.token.ReadableNetworkStakingRewardsStore;
import com.hedera.node.app.service.token.ReadableNodeRewardsStore;
import com.hedera.node.config.data.NodesConfig;
import com.swirlds.state.spi.ReadableSingletonState;
import com.swirlds.state.spi.ReadableStates;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
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

    public Long calculateTotalReward(
            final List<Long> activeNodeIds,
            final long avgNodeFeeCollected,
            final long payerBalance,
            final NodesConfig nodeConfig) {
        final var rewardPerNode = Math.min(nodeConfig.minNodeReward() - avgNodeFeeCollected, 0L);
        final var totalRewardToBePaid = activeNodeIds.size() * rewardPerNode;
        return Math.min(totalRewardToBePaid, payerBalance);
    }

    public List<Long> getActiveNodeIds(final List<RosterEntry> rosterEntries, final NodesConfig nodeConfig) {
        final List<Long> activeNodeIds = new ArrayList<>();
        final var activePercent = nodeConfig.activeRoundsPercent();

        final var nodeActivities = get().nodeActivities().stream()
                .collect(Collectors.toMap(NodeActivity::nodeId, NodeActivity::numMissedJudgeRounds));

        for (var entry : rosterEntries) {
            if (!nodeActivities.containsKey(entry.nodeId())) {
                activeNodeIds.add(entry.nodeId());
            } else if (isActive(nodeActivities.get(entry.nodeId()), activePercent, numRoundsInStakingPeriod())) {
                activeNodeIds.add(entry.nodeId());
            }
        }
        return activeNodeIds;
    }

    private boolean isActive(
            final long numRoundsMissedNodeJudges, final int activePercent, final long numRoundsInStakingPeriod) {
        return numRoundsMissedNodeJudges <= numRoundsInStakingPeriod * activePercent / 100;
    }
}
