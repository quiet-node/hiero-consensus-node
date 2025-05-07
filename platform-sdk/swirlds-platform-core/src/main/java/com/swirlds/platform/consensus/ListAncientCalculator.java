package com.swirlds.platform.consensus;

import com.hedera.hapi.platform.state.ConsensusSnapshot;
import com.hedera.hapi.platform.state.MinimumJudgeInfo;
import com.swirlds.logging.legacy.LogMarker;
import com.swirlds.platform.internal.EventImpl;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Objects;
import java.util.stream.LongStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hiero.consensus.model.event.AncientMode;
import org.hiero.consensus.model.event.EventConstants;
import org.hiero.consensus.model.hashgraph.ConsensusConstants;

public class ListAncientCalculator {
    private static final Logger logger = LogManager.getLogger(ListAncientCalculator.class);
    /** consensus configuration */
    private final ConsensusConfig config;
    /** the ancient mode currently in use */
    private final AncientMode ancientMode;
    /** stores the minimum judge ancient identifier for all decided and non-expired rounds */
    private final SequentialRingBuffer<MinimumJudgeInfo> minimumJudgeStorage;

    /** the current threshold below which all events are ancient */
    private long ancientThreshold = EventConstants.ANCIENT_THRESHOLD_UNDEFINED;
    private final RoundNumbers roundNumbers;

    public ListAncientCalculator(
            @NonNull final ConsensusConfig config,
            @NonNull final AncientMode ancientMode
    ) {
        this.config = Objects.requireNonNull(config);
        this.ancientMode = Objects.requireNonNull(ancientMode);
        this.minimumJudgeStorage =
                new SequentialRingBuffer<>(ConsensusConstants.ROUND_FIRST, config.roundsExpired() * 2);
        this.roundNumbers = RoundNumbers.genesis(config);
    }

    public void reset(){
        minimumJudgeStorage.reset(ConsensusConstants.ROUND_FIRST);
        roundNumbers.reset();
        updateAncientThreshold();
    }

    /**
     * Used when loading rounds from a starting point (a signed state). It will create rounds with their minimum ancient
     * indicator numbers, but we won't know about the witnesses in these rounds. We also don't care about any other
     * information except for minimum ancient indicator since these rounds have already been decided beforehand.
     *
     * @param snapshot contains a list of round numbers and round ancient indicator pairs, in ascending round numbers
     */
    public void loadSnapshot(@NonNull final ConsensusSnapshot snapshot) {
        minimumJudgeStorage.reset(snapshot.minimumJudgeInfoList().getFirst().round());
        for (final MinimumJudgeInfo minimumJudgeInfo : snapshot.minimumJudgeInfoList()) {
            minimumJudgeStorage.add(minimumJudgeInfo.round(), minimumJudgeInfo);
        }
        roundNumbers.setElectionRound(minimumJudgeStorage.getLatest().round() + 1);
        updateAncientThreshold();
    }

    /**
     * Notifies the instance that the current elections have been decided
     */
    public void currentElectionDecided(@NonNull final List<EventImpl> judges) {
        long minJudgeValue = Long.MAX_VALUE;
        for (final EventImpl judge : judges) {
            final long judgeValue = ancientMode == AncientMode.GENERATION_THRESHOLD ?judge.getGeneration():judge.getBirthRound();
            minJudgeValue = Math.min(minJudgeValue, judgeValue);
        }
        minimumJudgeStorage.add(roundNumbers.getElectionRound(), new MinimumJudgeInfo(roundNumbers.getElectionRound(), minJudgeValue));
        roundNumbers.incrementElectionRound();
        // Delete the oldest rounds with round number which is expired
        minimumJudgeStorage.removeOlderThan(roundNumbers.getElectionRound() - config.roundsExpired());
        updateAncientThreshold();
    }

    /**
     * Update the current ancient threshold based on the latest round decided.
     */
    public void updateAncientThreshold() {
        if (!roundNumbers.isAnyRoundDecided()) {
            // if no round has been decided, no events are ancient yet
            ancientThreshold = EventConstants.ANCIENT_THRESHOLD_UNDEFINED;
            return;
        }
        final long nonAncientRound =
                RoundCalculationUtils.getOldestNonAncientRound(config.roundsNonAncient(), roundNumbers.getLastRoundDecided());
        final MinimumJudgeInfo info = minimumJudgeStorage.get(nonAncientRound);
        ancientThreshold = info.minimumJudgeAncientThreshold();
    }

    /**
     * @return A list of {@link MinimumJudgeInfo} for all decided and non-ancient rounds
     */
    public @NonNull List<MinimumJudgeInfo> getMinimumJudgeInfoList() {
        return LongStream.range(roundNumbers.getOldestNonAncientRound(), roundNumbers.getElectionRound())
                .mapToObj(this::getMinimumJudgeIndicator)
                .filter(Objects::nonNull)
                .toList();
    }

    private MinimumJudgeInfo getMinimumJudgeIndicator(final long round) {
        final MinimumJudgeInfo minimumJudgeInfo = minimumJudgeStorage.get(round);
        if (minimumJudgeInfo == null) {
            logger.error(
                    LogMarker.EXCEPTION.getMarker(),
                    "Missing round {}. Fame decided below {}, oldest non-ancient round {}",
                    round,
                    roundNumbers.getElectionRound(),
                    roundNumbers.getOldestNonAncientRound());
            return null;
        }
        return minimumJudgeInfo;
    }

    /**
     * Returns the threshold of all the judges that are not in ancient rounds. This is either a generation value or a
     * birth round value, depending on the ancient mode configured. If no judges are ancient, returns
     * {@link EventConstants#FIRST_GENERATION} or {@link ConsensusConstants#ROUND_FIRST} depending on the ancient mode.
     *
     * @return the threshold
     */
    public long getAncientThreshold() {
        return ancientThreshold;
    }

    /**
     * Similar to {@link #getAncientThreshold()} but for expired rounds.
     *
     * @return the threshold for expired rounds
     */
    public long getExpiredThreshold() {
        final MinimumJudgeInfo info = minimumJudgeStorage.get(minimumJudgeStorage.minIndex());
        return info == null ? EventConstants.ANCIENT_THRESHOLD_UNDEFINED : info.minimumJudgeAncientThreshold();
    }
}
