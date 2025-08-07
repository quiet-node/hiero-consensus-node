package com.swirlds.platform.components;

import com.swirlds.platform.gossip.FallenBehindManagerImpl;
import com.swirlds.platform.reconnect.ReconnectController;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.consensus.gossip.FallenBehindManager;
import org.hiero.consensus.model.node.NodeId;

public class DefaultPlatfromReconnecter implements PlatfromReconnecter {

    FallenBehindManager fallenBehindManager = new FallenBehindManagerImpl(
            0, // numNeighbors, this is not used in this implementation
            null, // statusActionSubmitter, not used in this implementation
            null // config, not used in this implementation
    );
    private ReconnectController reconnectController;

    //        final ReconnectPlatformHelper reconnectPlatformHelper = new ReconnectPlatformHelperImpl(
    //                gossipController::pause,
    //                clearAllPipelinesForReconnect::run,
    //                swirldStateManager::getConsensusState,
    //                state -> {
    //                    loadReconnectState.accept(state);
    //                },
    //                platformContext.getMerkleCryptography());

    //        this.reconnectController = new ReconnectController(
    //                reconnectConfig,
    //                threadManager,
    //                reconnectPlatformHelper,
    //                reconnectNetworkHelper,
    //                gossipController::resume,
    //                throttle,
    //                new DefaultSignedStateValidator(platformContext, platformStateFacade));
    @Override
    public void reportFallenBehind(@NonNull final NodeId id) {
        final boolean previouslyFallenBehind = fallenBehindManager.hasFallenBehind();
        reportFallenBehind(id);
        final boolean nowFallenBehind = fallenBehindManager.hasFallenBehind();

        if (!previouslyFallenBehind && nowFallenBehind) {
            // update status
            // start reconnect process
        }
    }

    @Override
    public void clearFallenBehind(@NonNull final NodeId id) {

    }

    private void executeReconnect() {
        reconnectController.start();
        //loadReconnectState.accept(state);
        // fallenBehindManager.resetFallenBehind();
    }
}
