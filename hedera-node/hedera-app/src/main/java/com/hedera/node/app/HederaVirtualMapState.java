// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app;

import static com.swirlds.platform.state.service.PlatformStateFacade.DEFAULT_PLATFORM_STATE_FACADE;

import com.hedera.hapi.platform.state.ConsensusSnapshot;
import com.swirlds.common.context.PlatformContext;
import com.swirlds.common.merkle.MerkleNode;
import com.swirlds.platform.state.MerkleNodeState;
import com.swirlds.platform.state.PlatformStateAccessor;
import com.swirlds.state.State;
import com.swirlds.state.merkle.VirtualMapState;
import com.swirlds.virtualmap.VirtualMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.base.constructable.ConstructableIgnored;

/**
 * This class sole purpose is to extend the {@link VirtualMapState} class and implement the {@link MerkleNodeState}.
 * Technically, {@link VirtualMapState} is already implementing {@link State} and {@link MerkleNode} but it does not
 * implement the {@link MerkleNodeState} interface. This class is merely a connector of these two interfaces.
 */
@ConstructableIgnored
public class HederaVirtualMapState extends VirtualMapState<HederaVirtualMapState> implements MerkleNodeState {

    /**
     * Constructs a {@link HederaVirtualMapState}.
     *
     * @param platformContext the platform context
     */
    public HederaVirtualMapState(@NonNull final PlatformContext platformContext) {
        super(platformContext);
    }

    /**
     * Constructs a {@link HederaVirtualMapState} using the specified {@link VirtualMap}.
     *
     * @param virtualMap      the virtual map whose metrics must already be registered
     * @param platformContext the platform context
     */
    public HederaVirtualMapState(@NonNull final VirtualMap virtualMap, @NonNull final PlatformContext platformContext) {
        super(virtualMap, platformContext);
    }

    protected HederaVirtualMapState(@NonNull final HederaVirtualMapState from) {
        super(from);
    }
    /**
     * {@inheritDoc}
     */
    @Override
    protected HederaVirtualMapState copyingConstructor() {
        return new HederaVirtualMapState(this);
    }

    /**
     * Creates a new instance of {@link HederaVirtualMapState} with the specified {@link VirtualMap}.
     *
     * @param virtualMap      the virtual map whose metrics must already be registered
     * @param platformContext the platform context
     * @return a new instance of {@link HederaVirtualMapState}
     */
    @Override
    protected HederaVirtualMapState newInstance(
            @NonNull final VirtualMap virtualMap, @NonNull final PlatformContext platformContext) {
        return new HederaVirtualMapState(virtualMap, platformContext);
    }

    @Override
    public long getRound() {
        final ConsensusSnapshot consensusSnapshot = DEFAULT_PLATFORM_STATE_FACADE.consensusSnapshotOf(this);
        return consensusSnapshot == null ? PlatformStateAccessor.GENESIS_ROUND : consensusSnapshot.round();
    }
}
