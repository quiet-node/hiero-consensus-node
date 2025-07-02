// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.ids.schemas;

import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.platform.state.NodeId;
import com.swirlds.state.lifecycle.MigrationContext;
import com.swirlds.state.lifecycle.Schema;
import com.swirlds.state.lifecycle.StateDefinition;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Set;

public class V0650EntityIdSchema extends Schema {
    /**
     * The version of the schema.
     */
    private static final SemanticVersion VERSION =
            SemanticVersion.newBuilder().major(0).minor(65).patch(0).build();

    public static final String NODE_ID_KEY = "NODE_ID";

    public V0650EntityIdSchema() {
        super(VERSION);
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public Set<StateDefinition> statesToCreate() {
        return Set.of(StateDefinition.singleton(NODE_ID_KEY, NodeId.PROTOBUF));
    }

    @Override
    public void migrate(@NonNull final MigrationContext ctx) {
        final var nodeIdState = ctx.newStates().getSingleton(NODE_ID_KEY);
        nodeIdState.put(NodeId.DEFAULT);
    }
}
