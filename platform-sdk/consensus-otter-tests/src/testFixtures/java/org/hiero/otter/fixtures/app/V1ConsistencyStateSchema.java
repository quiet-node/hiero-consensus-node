package org.hiero.otter.fixtures.app;

import com.hedera.hapi.node.base.SemanticVersion;
import com.swirlds.state.lifecycle.Schema;
import com.swirlds.state.lifecycle.StateDefinition;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Set;
import org.hiero.otter.fixtures.app.state.ConsistencyState;
import org.jetbrains.annotations.NotNull;

public class V1ConsistencyStateSchema extends Schema {

    public static final String CONSISTENCY_STATE_KEY = "CONSISTENCY_STATE";

    /**
     * Create a new instance
     *
     * @param version The version of this schema
     */
    protected V1ConsistencyStateSchema(@NotNull final SemanticVersion version) {
        super(version);
    }

    @NonNull
    @Override
    public Set<StateDefinition> statesToCreate() {
        return Set.of(StateDefinition.singleton(CONSISTENCY_STATE_KEY, ConsistencyState.PROTOBUF));
    }
}
