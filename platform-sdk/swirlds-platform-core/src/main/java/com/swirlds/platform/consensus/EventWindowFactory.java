package com.swirlds.platform.consensus;

import com.hedera.hapi.platform.state.ConsensusSnapshot;
import org.hiero.consensus.model.event.AncientMode;
import org.hiero.consensus.model.hashgraph.EventWindow;

public class EventWindowFactory {
    public static EventWindow create(
            final ConsensusConfig config,
            final AncientMode ancientMode,
            final ConsensusSnapshot snapshot
    ) {
        final long threshold;
        if (ancientMode == AncientMode.BIRTH_ROUND_THRESHOLD && !config.useListBirthRoundAncient()) {
            threshold = RoundCalculationUtils.getOldestNonAncientRound(config.roundsNonAncient(), snapshot.round());
        } else {
            threshold = RoundCalculationUtils.getAncientThreshold(config.roundsNonAncient(), snapshot);
        }
        return new EventWindow(
                snapshot.round(),
                threshold,
                threshold,
                ancientMode);

    }
}
