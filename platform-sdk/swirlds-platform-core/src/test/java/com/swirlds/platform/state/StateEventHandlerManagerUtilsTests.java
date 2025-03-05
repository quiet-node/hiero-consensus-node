// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.state;

import static com.swirlds.platform.test.fixtures.state.FakeConsensusStateEventHandler.FAKE_CONSENSUS_STATE_EVENT_HANDLER;
import static com.swirlds.platform.test.fixtures.state.TestPlatformStateFacade.TEST_PLATFORM_STATE_FACADE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;

import com.swirlds.platform.metrics.StateMetrics;
import com.swirlds.platform.system.BasicSoftwareVersion;
import com.swirlds.platform.test.fixtures.state.TestMerkleStateRoot;
import com.swirlds.state.State;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StateEventHandlerManagerUtilsTests {

    @BeforeEach
    void setup() {}

    @Test
    void testFastCopyIsMutable() {

        final MerkleNodeState state = new TestMerkleStateRoot();
        FAKE_CONSENSUS_STATE_EVENT_HANDLER.initPlatformState(state);
        state.getRoot().reserve();
        final StateMetrics stats = mock(StateMetrics.class);
        final State result =
                SwirldStateManagerUtils.fastCopy(state, stats, new BasicSoftwareVersion(1), TEST_PLATFORM_STATE_FACADE);

        assertFalse(result.isImmutable(), "The copy state should be mutable.");
        assertEquals(
                1,
                state.getRoot().getReservationCount(),
                "Fast copy should not change the reference count of the state it copies.");
        assertEquals(
                1,
                state.getRoot().getReservationCount(),
                "Fast copy should return a new state with a reference count of 1.");
    }
}
