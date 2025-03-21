// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.service.token;

import com.hedera.hapi.node.state.roster.RosterEntry;
import com.hedera.hapi.node.state.token.NodeRewards;
import java.util.List;

/**
 * Provides read-only methods for interacting with the underlying data storage mechanisms for
 * working with node rewards.
 */
public interface ReadableNodeRewardsStore {
    /**
     * Returns the {link NodeRewards} in state.
     *
     * @return the {link NodeRewards} in state
     */
    NodeRewards get();

    /**
     * Returns the number of rounds in the staking period.
     *
     * @return the number of rounds in the staking period
     */
    long numRoundsInStakingPeriod();

    /**
     * Calculates all the active nodes based on the active percent. A node is considered active if it has missed
     * creating judges in less than the active percent of the total number of rounds in the staking period.
     *
     * @param rosterEntries the list of roster entries
     * @param activePercent the active percent
     * @return the list of active node ids
     */
    List<Long> getActiveNodeIds(final List<RosterEntry> rosterEntries, final int activePercent);
}
