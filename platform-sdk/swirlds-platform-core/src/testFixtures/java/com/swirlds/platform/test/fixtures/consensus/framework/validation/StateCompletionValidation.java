// SPDX-License-Identifier: Apache-2.0
package com.swirlds.platform.test.fixtures.consensus.framework.validation;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.swirlds.platform.state.signed.ReservedSignedState;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Validate if this object contains a complete set of signatures
 */
public class StateCompletionValidation implements ConsensusStateValidation {

    /**
     * Validate that the state is complete.
     *
     * @param reservedSignedState the state to validate
     */
    @Override
    public void validate(@NonNull ReservedSignedState reservedSignedState) {
        assertTrue(reservedSignedState.get().isComplete());
    }
}
