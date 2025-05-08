package com.swirlds.platform.consensus;

import org.hiero.consensus.model.hashgraph.ConsensusConstants;

public class RoundNumbers {
    /** consensus configuration */
    private final ConsensusConfig config;
    private long electionRound;

    private RoundNumbers(final ConsensusConfig config, final long electionRound) {
        this.config = config;
        this.electionRound = electionRound;
    }

    public static RoundNumbers genesis(final ConsensusConfig config) {
        return new RoundNumbers(config, ConsensusConstants.ROUND_FIRST);
    }

    public void incrementElectionRound() {
        electionRound++;
    }

    public void setElectionRound(final long electionRound) {
        this.electionRound = electionRound;
    }

    public void setLastDecidedRound(final long lastDecidedRound) {
        this.electionRound = lastDecidedRound + 1;
    }

    public void reset() {
        electionRound = ConsensusConstants.ROUND_FIRST;
    }

    public long getElectionRound() {
        return electionRound;
    }

    /**
     * @return true if we have decided fame for at least one round, this is false only at genesis
     */
    public boolean isAnyRoundDecided() {
        return electionRound > ConsensusConstants.ROUND_FIRST;
    }

    public long getLastRoundDecided() {
        return electionRound - 1;
    }

    /**
     * Returns the oldest round that is non-ancient. If no round is ancient, then it will return the first round ever
     *
     * @return oldest non-ancient number
     */
    public long getOldestNonAncientRound() {
        return RoundCalculationUtils.getOldestNonAncientRound(config.roundsNonAncient(), getLastRoundDecided());
    }
}
