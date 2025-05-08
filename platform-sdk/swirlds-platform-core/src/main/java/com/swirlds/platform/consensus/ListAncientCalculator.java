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

public class ListAncientCalculator implements AncientCalculator {
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

    @Override
    public void reset(){
        minimumJudgeStorage.reset(ConsensusConstants.ROUND_FIRST);
        roundNumbers.reset();
        updateAncientThreshold();
    }

    @Override
    public void loadSnapshot(@NonNull final ConsensusSnapshot snapshot) {
        minimumJudgeStorage.reset(snapshot.minimumJudgeInfoList().getFirst().round());
        for (final MinimumJudgeInfo minimumJudgeInfo : snapshot.minimumJudgeInfoList()) {
            minimumJudgeStorage.add(minimumJudgeInfo.round(), minimumJudgeInfo);
        }
        roundNumbers.setElectionRound(minimumJudgeStorage.getLatest().round() + 1);
        updateAncientThreshold();
    }

    @Override
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
    private void updateAncientThreshold() {
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

    @NonNull
    @Override
    public List<MinimumJudgeInfo> getMinimumJudgeInfoList() {
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

    @Override
    public long getAncientThreshold() {
        return ancientThreshold;
    }

    @Override
    public long getExpiredThreshold() {
        final MinimumJudgeInfo info = minimumJudgeStorage.get(minimumJudgeStorage.minIndex());
        return info == null ? EventConstants.ANCIENT_THRESHOLD_UNDEFINED : info.minimumJudgeAncientThreshold();
    }
}
