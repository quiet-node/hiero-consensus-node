// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.test.fixtures.state;

import static com.swirlds.platform.test.fixtures.state.TestPlatformStateFacade.TEST_PLATFORM_STATE_FACADE;

import com.hedera.hapi.platform.state.ConsensusSnapshot;
import com.swirlds.common.context.PlatformContext;
import com.swirlds.platform.state.MerkleNodeState;
import com.swirlds.platform.state.PlatformStateAccessor;
import com.swirlds.platform.test.fixtures.virtualmap.VirtualMapUtils;
import com.swirlds.state.State;
import com.swirlds.state.merkle.VirtualMapState;
import com.swirlds.virtualmap.VirtualMap;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A test implementation of {@link State} backed by a single Virtual Map.
 */
public class TestVirtualMapState extends VirtualMapState<TestVirtualMapState> implements MerkleNodeState {

    public TestVirtualMapState(@NonNull final PlatformContext platformContext) {
        super(platformContext);
    }

    public TestVirtualMapState(@NonNull final VirtualMap virtualMap, @NonNull final PlatformContext platformContext) {
        super(virtualMap, platformContext);
    }

    protected TestVirtualMapState(@NonNull final TestVirtualMapState from) {
        super(from);
    }
    /**
     * {@inheritDoc}
     */
    @Override
    protected TestVirtualMapState copyingConstructor() {
        return new TestVirtualMapState(this);
    }

    @Override
    protected TestVirtualMapState newInstance(
            @NonNull final VirtualMap virtualMap, @NonNull final PlatformContext platformContext) {
        return new TestVirtualMapState(virtualMap, platformContext);
    }

    public static TestVirtualMapState createInstanceWithVirtualMapLabel(
            @NonNull final String virtualMapLabel, @NonNull final PlatformContext platformContext) {
        final var virtualMap = VirtualMapUtils.createVirtualMap(virtualMapLabel);
        return new TestVirtualMapState(virtualMap, platformContext);
    }

    @Override
    protected long getRound() {
        final ConsensusSnapshot consensusSnapshot = TEST_PLATFORM_STATE_FACADE.consensusSnapshotOf(this);
        return consensusSnapshot == null ? PlatformStateAccessor.GENESIS_ROUND : consensusSnapshot.round();
    }
}
