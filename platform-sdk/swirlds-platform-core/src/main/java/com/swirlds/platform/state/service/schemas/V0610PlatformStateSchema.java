// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.state.service.schemas;

import static com.swirlds.platform.state.service.schemas.V0540PlatformStateSchema.PLATFORM_STATE_KEY;
import static java.util.Objects.requireNonNull;

import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.platform.state.ConsensusSnapshot;
import com.hedera.hapi.platform.state.Judge;
import com.hedera.hapi.platform.state.PlatformState;
import com.swirlds.state.lifecycle.MigrationContext;
import com.swirlds.state.lifecycle.Schema;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Defines the {@link PlatformState} singleton and initializes it at genesis.
 */
public class V0610PlatformStateSchema extends Schema {
    private static final SemanticVersion VERSION =
            SemanticVersion.newBuilder().major(0).minor(61).patch(0).build();
    private static final Logger log = LogManager.getLogger(V0610PlatformStateSchema.class);
    private static final long SENTINEL_CREATOR_ID = 0L;

    public V0610PlatformStateSchema() {
        super(VERSION);
    }

    @Override
    public void migrate(@NonNull final MigrationContext ctx) {
        final var oldPlatformState = ctx.previousStates().<PlatformState>getSingleton(PLATFORM_STATE_KEY);
        final var newPlatformState = ctx.newStates().<PlatformState>getSingleton(PLATFORM_STATE_KEY);

        final var oldConsensusSnapshot = requireNonNull(oldPlatformState.get()).consensusSnapshot();
        final var judgeHashes = oldConsensusSnapshot.judgeHashes();
        final var judges = new ArrayList<Judge>();
        // Services don't need to depend on the creator ID for the first round after migration.
        // So, setting it to a sentinel value.
        for (final var judgeHash : judgeHashes) {
            judges.add(Judge.newBuilder()
                    .creatorId(SENTINEL_CREATOR_ID)
                    .judgeHash(judgeHash)
                    .build());
        }
        final var copy = requireNonNull(oldPlatformState.get())
                .copyBuilder()
                .consensusSnapshot(ConsensusSnapshot.newBuilder()
                        .consensusTimestamp(oldConsensusSnapshot.consensusTimestamp())
                        .round(oldConsensusSnapshot.round())
                        .minimumJudgeInfoList(oldConsensusSnapshot.minimumJudgeInfoList())
                        .nextConsensusNumber(oldConsensusSnapshot.nextConsensusNumber())
                        .judges(judges)
                        .build())
                .build();
        log.info(
                "Migrating platform state ConsensusSnapshot to version 0.61.0. "
                        + "Previous Judge Hashes {}, new judges {}",
                judgeHashes,
                judges);
        newPlatformState.put(copy);
    }
}
