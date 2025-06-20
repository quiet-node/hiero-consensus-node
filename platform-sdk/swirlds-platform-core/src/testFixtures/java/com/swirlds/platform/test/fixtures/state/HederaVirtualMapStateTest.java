// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.test.fixtures.state;

import com.swirlds.config.api.Configuration;
import com.swirlds.metrics.api.Metrics;
import com.swirlds.platform.state.MerkleNodeState;
import com.swirlds.platform.test.fixtures.virtualmap.VirtualMapUtils;
import com.swirlds.state.merkle.VirtualMapState;
import com.swirlds.virtualmap.VirtualMap;
import edu.umd.cs.findbugs.annotations.NonNull;

public class HederaVirtualMapStateTest extends VirtualMapState<HederaVirtualMapStateTest> implements MerkleNodeState {

    public HederaVirtualMapStateTest(@NonNull final Configuration configuration, @NonNull final Metrics metrics) {
        super(configuration, metrics);
    }

    public HederaVirtualMapStateTest(@NonNull final VirtualMap virtualMap) {
        super(virtualMap);
    }

    protected HederaVirtualMapStateTest(@NonNull final HederaVirtualMapStateTest from) {
        super(from);
    }
    /**
     * {@inheritDoc}
     */
    @Override
    protected HederaVirtualMapStateTest copyingConstructor() {
        return new HederaVirtualMapStateTest(this);
    }

    @Override
    protected HederaVirtualMapStateTest newInstance(@NonNull final VirtualMap virtualMap) {
        return new HederaVirtualMapStateTest(virtualMap);
    }

    public static HederaVirtualMapStateTest createInstanceWithVirtualMapLabel(@NonNull final String virtualMapLabel) {
        final var virtualMap = VirtualMapUtils.createVirtualMap(virtualMapLabel);
        return new HederaVirtualMapStateTest(virtualMap);
    }
}
