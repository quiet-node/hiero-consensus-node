package com.swirlds.platform.components;

import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.consensus.model.node.NodeId;

public interface PlatfromReconnecter {
    void reportFallenBehind(@NonNull final NodeId id);

    /**
     * Notify the fallen behind manager that a node has reported that node is providing us with events we need. This
     * means we are not in fallen behind state against that node.
     *
     * @param id the id of the node who is providing us with up to date events
     */
    void clearFallenBehind(@NonNull NodeId id);
}
