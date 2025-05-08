package com.swirlds.platform.consensus;

import com.hedera.hapi.platform.event.EventDescriptor;
import com.hedera.hapi.platform.state.ConsensusSnapshot;
import com.hedera.hapi.platform.state.MinimumJudgeInfo;
import com.swirlds.platform.internal.EventImpl;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import org.hiero.consensus.model.hashgraph.ConsensusConstants;

public class BirthRoundAncientCalculator implements AncientCalculator {
    final ConsensusConfig config;
    private long minJudgeBirthRound = ConsensusConstants.ROUND_UNDEFINED;


    public BirthRoundAncientCalculator(final ConsensusConfig config) {
        this.config = config;
    }

    @Override
    public void reset() {
        minJudgeBirthRound = ConsensusConstants.ROUND_UNDEFINED;
    }

    @Override
    public void loadSnapshot(@NonNull final ConsensusSnapshot snapshot) {
        minJudgeBirthRound = snapshot.judges().stream().mapToLong(EventDescriptor::birthRound).min().orElseThrow(
                () -> new IllegalArgumentException("Snapshot must contain at least one judge")
        );
    }

    @Override
    public void currentElectionDecided(@NonNull final List<EventImpl> judges) {
        minJudgeBirthRound = judges.stream()
                .mapToLong(EventImpl::getBirthRound)
                .min()
                .orElseThrow(() -> new IllegalArgumentException("At least one judge should be supplied"));
    }

    @NonNull
    @Override
    public List<MinimumJudgeInfo> getMinimumJudgeInfoList() {
        return List.of();
    }

    @Override
    public long getAncientThreshold() {
        return RoundCalculationUtils.getOldestNonAncientRound(
                config.roundsNonAncient(),
                minJudgeBirthRound
        );
    }

    @Override
    public long getExpiredThreshold() {
        return RoundCalculationUtils.getOldestNonAncientRound(
                config.roundsExpired(),
                minJudgeBirthRound
        );
    }
}
