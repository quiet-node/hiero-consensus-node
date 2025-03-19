// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.service.token.impl.schemas;

import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.state.token.NodeRewards;
import com.swirlds.state.lifecycle.MigrationContext;
import com.swirlds.state.lifecycle.Schema;
import com.swirlds.state.lifecycle.StateDefinition;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Set;

public class V0610TokenSchema extends Schema {
    public static final String NODE_REWARDS_KEY = "NODE_REWARDS";

    private static final SemanticVersion VERSION =
            SemanticVersion.newBuilder().major(0).minor(61).patch(0).build();

    public V0610TokenSchema() {
        super(VERSION);
    }

    @SuppressWarnings("rawtypes")
    @NonNull
    @Override
    public Set<StateDefinition> statesToCreate() {
        return Set.of(StateDefinition.singleton(NODE_REWARDS_KEY, NodeRewards.PROTOBUF));
    }

    @Override
    public void migrate(@NonNull final MigrationContext ctx) {
        if (ctx.isGenesis()) {
            final var nodeRewardsState = ctx.newStates().getSingleton(NODE_REWARDS_KEY);
            nodeRewardsState.put(NodeRewards.DEFAULT);
        }
    }
}
