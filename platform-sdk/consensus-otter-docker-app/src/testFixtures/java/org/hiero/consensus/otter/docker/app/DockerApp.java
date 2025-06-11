package org.hiero.consensus.otter.docker.app;

import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.platform.event.StateSignatureTransaction;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.platform.state.ConsensusStateEventHandler;
import com.swirlds.platform.state.MerkleNodeState;
import com.swirlds.platform.system.Platform;
import com.swirlds.platform.system.SwirldMain;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.consensus.model.node.NodeId;

public class DockerApp implements SwirldMain<MerkleNodeState> {
    @Override
    public void init(@NonNull final Platform platform, @NonNull final NodeId selfId) {

    }

    @Override
    public void run() {

    }

    @NonNull
    @Override
    public MerkleNodeState newStateRoot() {
        return null;
    }

    @Override
    public ConsensusStateEventHandler<MerkleNodeState> newConsensusStateEvenHandler() {
        return null;
    }

    @NonNull
    @Override
    public SemanticVersion getSemanticVersion() {
        return null;
    }

    @NonNull
    @Override
    public Bytes encodeSystemTransaction(@NonNull final StateSignatureTransaction transaction) {
        return null;
    }
}
