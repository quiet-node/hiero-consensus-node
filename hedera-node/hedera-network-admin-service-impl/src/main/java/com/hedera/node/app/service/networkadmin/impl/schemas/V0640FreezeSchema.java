// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.service.networkadmin.impl.schemas;

import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.state.blockstream.FreezeInfo;
import com.hedera.node.app.service.networkadmin.FreezeService;
import com.swirlds.state.lifecycle.MigrationContext;
import com.swirlds.state.lifecycle.Schema;
import com.swirlds.state.lifecycle.StateDefinition;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Initial mod-service schema for the {@link FreezeService}.
 */
public class V0640FreezeSchema extends Schema {
    private static final Logger LOGGER = LogManager.getLogger(V0640FreezeSchema.class);
    public static final String FREEZE_INFO_KEY = "FREEZE_INFO";

    /**
     * The version of the schema.
     */
    private static final SemanticVersion VERSION =
            SemanticVersion.newBuilder().major(0).minor(64).patch(0).build();

    /**
     * Constructs a new {@link V0640FreezeSchema}.
     */
    public V0640FreezeSchema() {
        super(VERSION);
    }

    @NonNull
    @Override
    @SuppressWarnings("rawtypes")
    public Set<StateDefinition> statesToCreate() {
        return Set.of(StateDefinition.singleton(FREEZE_INFO_KEY, FreezeInfo.PROTOBUF));
    }

    @Override
    public void migrate(@NonNull final MigrationContext ctx) {
        final var lastFreezeRoundKeyState = ctx.newStates().<FreezeInfo>getSingleton(FREEZE_INFO_KEY);
        final var freezeBuilder = FreezeInfo.newBuilder();
        if (ctx.isGenesis()) {
            lastFreezeRoundKeyState.put(freezeBuilder.lastFreezeRound(0L).build());
        } else {
            LOGGER.info(
                    "V0640FreezeSchema is being applied during an upgrade and FreezeInfo is not set. "
                            + "Setting last freeze round to the current round number: {}.",
                    ctx.roundNumber());
            lastFreezeRoundKeyState.put(
                    freezeBuilder.lastFreezeRound(ctx.roundNumber()).build());
        }
    }
}
