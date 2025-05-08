// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.consensus;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import org.hiero.consensus.config.EventConfig;

/**
 * Configuration for the consensus algorithm
 *
 * @param roundsNonAncient         The number of consensus rounds that are defined to be non-ancient. There can be more
 *                                 non-ancient rounds, but these rounds will not have reached consensus. Once consensus
 *                                 is reached on a new round (i.e.,fame is decided for all its witnesses), another round
 *                                 will become ancient. Events, whose generation is older than the last non-ancient
 *                                 round generation, are ancient. If they don't have consensus yet, they're stale, and
 *                                 will never reach consensus and never have their transactions handled.
 * @param roundsExpired            Events this many rounds old are expired, and can be deleted from memory
 * @param useListBirthRoundAncient If {@link EventConfig#useBirthRoundAncientThreshold()} is set to false, this value
 *                                 has no effect. If we use birth rounds to determine the ancient and expired
 *                                 thresholds, then this config changes how they are calculated.<p> If
 *                                 useListBirthRoundAncient is set to true,
 *                                 {@link com.hedera.hapi.platform.state.ConsensusSnapshot} will store a list of
 *                                 thresholds for each round. This list will also be stored internally within
 *                                 {@link com.swirlds.platform.Consensus}. This list will define the ancient threshold.
 *                                 <p> If useListBirthRoundAncient is set to false, the ancient threshold will be just
 *                                 an offset from the judges in the latest rounds decided.
 * @param coinFreq                 a coin round happens every coinFreq rounds during an election (every other one is all
 *                                 true)
 */
@ConfigData("consensus")
public record ConsensusConfig(
        @ConfigProperty(defaultValue = "26") int roundsNonAncient,
        @ConfigProperty(defaultValue = "1000") int roundsExpired,
        @ConfigProperty(defaultValue = "false") boolean useListBirthRoundAncient,
        @ConfigProperty(defaultValue = "12") int coinFreq) {
}
