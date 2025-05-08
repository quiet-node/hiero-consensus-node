package com.swirlds.platform.consensus;

import com.hedera.hapi.platform.state.ConsensusSnapshot;
import com.hedera.hapi.platform.state.MinimumJudgeInfo;
import com.swirlds.platform.internal.EventImpl;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import org.hiero.consensus.model.event.EventConstants;
import org.hiero.consensus.model.hashgraph.ConsensusConstants;

public interface AncientCalculator {
    void reset();

    /**
     * Used when loading rounds from a starting point (a signed state). It will create rounds with their minimum ancient
     * indicator numbers, but we won't know about the witnesses in these rounds. We also don't care about any other
     * information except for minimum ancient indicator since these rounds have already been decided beforehand.
     *
     * @param snapshot contains a list of round numbers and round ancient indicator pairs, in ascending round numbers
     */
    void loadSnapshot(@NonNull ConsensusSnapshot snapshot);

    /**
     * Notifies the instance that the current elections have been decided
     */
    void currentElectionDecided(@NonNull List<EventImpl> judges);

    /**
     * @return A list of {@link MinimumJudgeInfo} for all decided and non-ancient rounds
     */
    @NonNull
    List<MinimumJudgeInfo> getMinimumJudgeInfoList();

    /**
     * Returns the threshold of all the judges that are not in ancient rounds. This is either a generation value or a
     * birth round value, depending on the ancient mode configured. If no judges are ancient, returns
     * {@link EventConstants#FIRST_GENERATION} or {@link ConsensusConstants#ROUND_FIRST} depending on the ancient mode.
     *
     * @return the threshold
     */
    long getAncientThreshold();

    /**
     * Similar to {@link #getAncientThreshold()} but for expired rounds.
     *
     * @return the threshold for expired rounds
     */
    long getExpiredThreshold();
}
