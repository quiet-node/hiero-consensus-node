package com.swirlds.platform.consensus;

import com.hedera.hapi.platform.state.ConsensusSnapshot;
import com.hedera.hapi.platform.state.MinimumJudgeInfo;
import com.swirlds.config.api.Configuration;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.NoSuchElementException;
import org.hiero.consensus.config.EventConfig;
import org.hiero.consensus.model.event.AncientMode;
import org.hiero.consensus.model.hashgraph.EventWindow;

public class EventWindowFactory {

    public static EventWindow create(
            final Configuration configuration,
            final ConsensusSnapshot snapshot) {
        return create(
                configuration.getConfigData(ConsensusConfig.class),
                configuration.getConfigData(EventConfig.class).getAncientMode(),
                snapshot
        );
    }

    public static EventWindow create(
            final ConsensusConfig config,
            final AncientMode ancientMode,
            final ConsensusSnapshot snapshot) {
        final long threshold;
        if (ancientMode == AncientMode.BIRTH_ROUND_THRESHOLD && !config.useListBirthRoundAncient()) {
            threshold = RoundCalculationUtils.getOldestNonAncientRound(config.roundsNonAncient(), snapshot.round());
        } else {
            threshold = getAncientThreshold(config.roundsNonAncient(), snapshot);
        }
        return new EventWindow(
                snapshot.round(),
                threshold,
                threshold,
                ancientMode);

    }

    /**
     * Returns the minimum generation below which all events are ancient
     *
     * @param roundsNonAncient the number of non-ancient rounds
     * @return minimum non-ancient generation
     */
    private static long getAncientThreshold(final int roundsNonAncient, @NonNull final ConsensusSnapshot snapshot) {
        final long oldestNonAncientRound =
                RoundCalculationUtils.getOldestNonAncientRound(roundsNonAncient, snapshot.round());
        return getMinimumJudgeAncientThreshold(oldestNonAncientRound, snapshot);
    }

    /**
     * The minimum ancient threshold of famous witnesses (i.e. judges) for the round specified. This method only looks
     * at non-ancient rounds contained within this state.
     *
     * @param round the round whose minimum judge ancient indicator will be returned
     * @return the minimum judge ancient indicator for the round specified
     * @throws NoSuchElementException if the minimum judge info information for this round is not contained withing this
     *                                state
     */
    private static long getMinimumJudgeAncientThreshold(final long round, @NonNull final ConsensusSnapshot snapshot) {
        for (final MinimumJudgeInfo info : snapshot.minimumJudgeInfoList()) {
            if (info.round() == round) {
                return info.minimumJudgeAncientThreshold();
            }
        }
        throw new NoSuchElementException("No minimum judge info found for round: " + round);
    }
}
