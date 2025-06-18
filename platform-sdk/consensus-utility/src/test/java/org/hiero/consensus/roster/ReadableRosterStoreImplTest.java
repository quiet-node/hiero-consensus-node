// SPDX-License-Identifier: Apache-2.0
package org.hiero.consensus.roster;

import static com.swirlds.state.BinaryStateUtils.getValidatedStateId;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.BDDMockito.given;

import com.hedera.hapi.node.state.roster.RosterState;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.state.BinaryState;
import com.swirlds.state.spi.ReadableSingletonState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ReadableRosterStoreImplTest {
    @Mock
    private BinaryState binaryState;

    private ReadableRosterStoreImpl subject;

    @BeforeEach
    void setUp() {
        subject = new ReadableRosterStoreImpl(binaryState);
    }

    @Test
    void nullCandidateRosterCasesPass() {
        given(binaryState.getSingleton(getValidatedStateId(RosterStateId.NAME, WritableRosterStore.ROSTER_STATES_KEY), RosterState.PROTOBUF))
                .willReturn(RosterState.DEFAULT);
        assertNull(subject.getCandidateRosterHash());
        assertNull(subject.getCandidateRosterHash());
    }

    @Test
    void nonNullCandidateRosterIsReturned() {
        final var fakeHash = Bytes.wrap("PRETEND");
        given(binaryState.getSingleton(getValidatedStateId(RosterStateId.NAME, WritableRosterStore.ROSTER_STATES_KEY), RosterState.PROTOBUF))
                .willReturn(RosterState.newBuilder().candidateRosterHash(fakeHash).build());
        assertEquals(fakeHash, subject.getCandidateRosterHash());
    }
}
