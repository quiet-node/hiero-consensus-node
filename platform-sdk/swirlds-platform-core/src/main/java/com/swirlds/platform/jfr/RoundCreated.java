package com.swirlds.platform.jfr;

import jdk.jfr.Timestamp;
import org.hiero.consensus.model.hashgraph.ConsensusRound;
import org.hiero.consensus.model.hashgraph.Round;

@jdk.jfr.Name("consensus.RoundCreated")
@jdk.jfr.Label("Consensus Round Created")
@jdk.jfr.Category({"Consensus"})
public class RoundCreated extends jdk.jfr.Event {
    long roundNum; // round number
    int  eventCount; // number of events in round
    int  txCount; // number of transactions in the round
    @Timestamp
    long consensusTimeStamp;

    public void setAll(ConsensusRound round) {
        roundNum = round.getRoundNum();
        eventCount = round.getEventCount();
        txCount = round.getNumAppTransactions();
        consensusTimeStamp = round.getConsensusTimestamp().getEpochSecond() * 1_000_000_000L +
                round.getConsensusTimestamp().getNano();
    }
}
